package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.axonframework.common.DateTimeUtils;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventEntryMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.TrackedEventDataMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.TrackedEventMessageMapper;
import org.axonframework.extensions.reactor.eventstore.statements.CleanGapsStatementBuilder;
import org.axonframework.extensions.reactor.eventstore.statements.FetchTrackedEventsStatementBuilder;
import org.axonframework.extensions.reactor.eventstore.statements.ReadEventDataWithGapsStatementBuilder;
import org.axonframework.extensions.reactor.eventstore.statements.ReadEventDataWithoutGapsStatementBuilder;
import org.axonframework.extensions.reactor.eventstore.utils.Tuple2;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * @author vtiwar27
 * @date 2020-11-09
 */
public class TrackedEventsReader {

    private final ConnectionFactory connectionFactory;
    private final EventSchema eventSchema;
    private final Class<?> dataType;
    private final Serializer serializer;
    private final DomainEventEntryMapper domainEventEntryMapper;
    private final DatabaseClient databaseClient;
    private final int batchSize;
    private int gapTimeout;
    private int gapCleaningThreshold;
    private final int maxGapOffset;
    private final long lowestGlobalSequence;
    private final TrackedEventDataMapper trackedEventDataMapper;
    private final TrackedEventMessageMapper trackedEventMessageMapper;
    private final EventUpcaster upcasterChain;
    private final TransactionalOperator transactionalOperator;
    private final boolean extendedGapCheckEnabled;
    private final ReadEventDataWithGapsStatementBuilder readEventDataWithGapsStatementBuilder;
    private final ReadEventDataWithoutGapsStatementBuilder readEventDataWithoutGapsStatementBuilder;
    private final CleanGapsStatementBuilder cleanGapsStatementBuilder;
    private final FetchTrackedEventsStatementBuilder fetchTrackedEventsStatementBuilder;


    public TrackedEventsReader(ConnectionFactory connectionFactory,
                               EventSchema eventSchema,
                               Class<?> dataType, Serializer serializer,
                               DomainEventEntryMapper domainEventEntryMapper,
                               DatabaseClient databaseClient,
                               TrackedEventDataMapper trackedEventDataMapper,
                               TrackedEventMessageMapper trackedEventMessageMapper,
                               TransactionalOperator transactionalOperator,
                               R2dbcEventStoreEngine.Builder builder) {
        this.connectionFactory = connectionFactory;
        this.eventSchema = eventSchema;
        this.dataType = dataType;
        this.serializer = serializer;
        this.domainEventEntryMapper = domainEventEntryMapper;
        this.databaseClient = databaseClient;
        this.batchSize = builder.batchSize;
        this.gapTimeout = builder.gapTimeout;
        this.gapCleaningThreshold = builder.gapCleaningThreshold;
        this.trackedEventDataMapper = trackedEventDataMapper;
        this.trackedEventMessageMapper = trackedEventMessageMapper;
        this.upcasterChain = builder.upcasterChain;
        this.transactionalOperator = transactionalOperator;
        this.cleanGapsStatementBuilder = builder.cleanGapsStatementBuilder;
        this.fetchTrackedEventsStatementBuilder = builder.fetchTrackedEventsStatementBuilder;
        this.readEventDataWithGapsStatementBuilder = builder.readEventDataWithGapsStatementBuilder;
        this.readEventDataWithoutGapsStatementBuilder = builder.readEventDataWithoutGapsStatementBuilder;
        this.maxGapOffset = builder.maxGapOffset;
        this.lowestGlobalSequence = builder.lowestGlobalSequence;
        this.extendedGapCheckEnabled = builder.extendedGapCheckEnabled;
    }


    public Flux<? extends TrackedEventData<?>> readEvents(TrackingToken lastToken, int batchSize) {

        return Mono.just(Optional.ofNullable(lastToken))
                .flatMap((token) -> {
                    //if (token.isPresent() || lastToken instanceof GapAwareTrackingToken) {
                    //    return Mono.error(new IllegalArgumentException("Unsupported token format: " + lastToken));
                    //}
                    return Mono.just(token);
                })
                .flatMap(token -> {
                    if (token.isPresent() && ((GapAwareTrackingToken) token.get()).getGaps().size() > gapCleaningThreshold) {
                        return cleanGaps(token.get());
                    } else {
                        return Mono.just(token);
                    }
                })
                .flatMap((token) -> Mono.just(token).contextWrite(context -> context.put("token", token)))
                .flatMapMany(gapAwareTrackingToken -> this.getTrackedEvents((GapAwareTrackingToken) gapAwareTrackingToken.orElse(null), batchSize))
                .collectList()
                .flatMapMany((results) -> {
                    if (extendedGapCheckEnabled && results.isEmpty()) {
                        return getTrackedEventData(lastToken);
                    } else {
                        return Flux.fromIterable(results);
                    }
                })
                .onErrorResume((error) -> Mono.deferContextual(contextView -> {
                    final Optional<Object> cleanedToken = contextView.getOrEmpty(GapAwareTrackingToken.class);
                    return Mono.error(new EventStoreException("Failed to read events from token", error));
                }))
                .as(this.transactionalOperator::transactional);

    }

    private Flux<TrackedEventData<?>> getTrackedEventData(TrackingToken lastToken) {
        return Flux.deferContextual(contextView -> {
            final GapAwareTrackingToken cleanedToken =
                    (GapAwareTrackingToken) lastToken;
            long index = cleanedToken == null ? -1 : cleanedToken.getIndex();
            return getTrackedEvents(index).flatMapMany(result -> {
                if (result.isPresent()) {
                    return getTrackedEvents(cleanedToken, (int) (result.get() - index));
                } else {
                    return Flux.empty();
                }
            });
        });
    }


    private Flux<TrackedEventData<?>> getTrackedEvents(GapAwareTrackingToken cleanedToken, int batchSize) {
        return this.databaseClient.inConnectionMany(connection -> {
            final Statement statement = this.readEventData(connection, cleanedToken, batchSize);
            return Flux.from(statement.execute()).flatMap(r -> {
                return r.map((this.trackedEventDataMapper::map));
            });
        }).collectList().flatMapMany((results) -> {
            List<TrackedEventData<?>> trackedEventDataList =
                    getTrackedEventData(cleanedToken, results);
            return Flux.fromIterable(trackedEventDataList);
        });

    }


    private Mono<Optional<Long>> getTrackedEvents(long index) {
        return databaseClient.inConnectionMany(connection -> {
            Statement statement = fetchTrackedEventsStatementBuilder.build(connection, eventSchema, index);
            return Flux.from(statement.execute()).flatMap(r -> r.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Long.class))));
        }).next();
    }

    private Mono<Optional<GapAwareTrackingToken>> cleanGaps(TrackingToken lastToken) {
        SortedSet<Long> gaps = ((GapAwareTrackingToken) lastToken).getGaps();
        return this.databaseClient.inConnection(connection -> {
            final Statement statement =
                    cleanGapsStatementBuilder.build(connection, eventSchema, gaps);
            return Flux.from(statement.execute()).flatMap(r -> r.map((row, rowMetaData) -> {
                Long sequenceNumber = row.get(eventSchema.globalIndexColumn(), Long.class);
                Instant timestamp =
                        DateTimeUtils.parseInstant(row.get(eventSchema.timestampColumn(), String.class));

                return new Tuple2<>(sequenceNumber, timestamp);
            })).collectList().flatMap((r) -> {
                GapAwareTrackingToken cleanToken = (GapAwareTrackingToken) lastToken;
                for (Tuple2<Long, Instant> instantTuple2 : r) {
                    if (gaps.contains(instantTuple2.getT1()) || instantTuple2.getT2().isAfter(gapTimeoutFrame())) {
                        // Filled a gap, should not continue cleaning up.
                        break;
                    }
                    if (gaps.contains(instantTuple2.getT1() - 1)) {
                        cleanToken = cleanToken.withGapsTruncatedAt(instantTuple2.getT1());
                    }
                }
                return Mono.just(Optional.ofNullable(cleanToken));
            });
        });
    }

    private List<TrackedEventData<?>> getTrackedEventData(GapAwareTrackingToken lastToken,
                                                          List<Tuple2<Long, DomainEventData<?>>> results) {
        List<TrackedEventData<?>> trackedEventDataList = new ArrayList<>();
        for (Tuple2<Long, DomainEventData<?>> result : results) {
            long globalSequence = result.getT1();
            final DomainEventData<?> domainEvent = result.getT2();
            boolean allowGaps = false;// domainEvent.getTimestamp().isAfter(gapTimeoutFrame());
            GapAwareTrackingToken token = lastToken;
            if (token == null) {
                token = GapAwareTrackingToken.newInstance(
                        globalSequence,
                        allowGaps
                                ? LongStream.range(Math.min(lowestGlobalSequence, globalSequence), globalSequence)
                                .boxed()
                                .collect(Collectors.toCollection(TreeSet::new))
                                : Collections.emptySortedSet()
                );
            } else {
                token = token.advanceTo(globalSequence, maxGapOffset);
                if (!allowGaps) {
                    token = token.withGapsTruncatedAt(globalSequence);
                }
            }
            trackedEventDataList.add(new TrackedDomainEventData<>(token, domainEvent));
        }
        return trackedEventDataList;
    }

    private Instant gapTimeoutFrame() {
        return GenericEventMessage.clock.instant().minus(gapTimeout, ChronoUnit.MILLIS);
    }

    private Statement readEventData(Connection connection, TrackingToken lastToken, int batchSize) {
        GapAwareTrackingToken previousToken = (GapAwareTrackingToken) lastToken;

        if (previousToken == null) {
            return readEventDataWithoutGapsStatementBuilder.build(connection, eventSchema, -1, batchSize);
        }

        List<Long> gaps = new ArrayList<>(previousToken.getGaps());
        long globalIndex = previousToken.getIndex();
        if (gaps.isEmpty()) {
            return readEventDataWithoutGapsStatementBuilder.build(connection, eventSchema, globalIndex, batchSize);
        }
        return readEventDataWithGapsStatementBuilder.build(connection, eventSchema, globalIndex, batchSize, gaps);
    }


}
