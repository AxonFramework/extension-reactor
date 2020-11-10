package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.eventsourcing.snapshotting.SnapshotFilter;
import org.axonframework.extensions.reactor.eventstore.BlockingReactiveEventStoreEngineSupport;
import org.axonframework.extensions.reactor.eventstore.R2dbSqlErrorCodeResolver;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventEntryMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.TrackedEventDataMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.TrackedEventMessageMapper;
import org.axonframework.extensions.reactor.eventstore.statements.*;
import org.axonframework.modelling.command.AggregateStreamCreationException;
import org.axonframework.modelling.command.ConcurrencyException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcaster;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * @author vtiwar27
 * @date 2020-09-30
 */


public class R2dbcEventStoreEngine implements ReactiveEventStoreEngine, BlockingReactiveEventStoreEngineSupport {

    private static final int DEFAULT_MAX_GAP_OFFSET = 10000;
    private static final long DEFAULT_LOWEST_GLOBAL_SEQUENCE = 1;
    private static final int DEFAULT_GAP_TIMEOUT = 60000;
    private static final int DEFAULT_GAP_CLEANING_THRESHOLD = 250;
    private static final boolean DEFAULT_EXTENDED_GAP_CHECK_ENABLED = true;


    private final ConnectionFactory connectionFactory;
    private final EventSchema eventSchema;
    private final Class<?> dataType;
    private final Serializer serializer;
    private final DomainEventEntryMapper domainEventEntryMapper;
    private final DatabaseClient databaseClient;
    private final int batchSize;
    private final TrackedEventDataMapper trackedEventDataMapper;
    private final TrackedEventMessageMapper trackedEventMessageMapper;
    private final EventUpcasterChain upcasterChain;
    private final TransactionalOperator transactionalOperator;
    private final PersistenceExceptionResolver persistenceExceptionResolver;

    private final TrackedEventsReader trackedEventsReader;
    private final DomainEventMapper domainEventMapper;
    private final AppendEventsStatementBuilder appendEventsStatementBuilder;
    private final AppendSnapshotStatementBuilder appendSnapshotStatementBuilder;
    private final ReadEventDataForAggregateStatementBuilder readEventDataForAggregateStatementBuilder;
    private final CreateTailTokenStatementBuilder createTailTokenStatementBuilder;
    private final CreateTokenAtStatementBuilder createTokenAtStatementBuilder;
    private final LastSequenceNumberForStatementBuilder lastSequenceNumberForStatementBuilder;
    private final ReadSnapshotDataStatementBuilder readSnapshotDataStatementBuilder;
    private final CreateHeadTokenStatementBuilder createHeadTokenStatementBuilder;
    private final DeleteSnapshotsStatementBuilder deleteSnapshotsStatementBuilder;


    public R2dbcEventStoreEngine(Builder builder) {
        this.connectionFactory = builder.connectionFactory;
        this.dataType = builder.dataType;
        this.serializer = builder.eventSerializer.get();
        this.domainEventEntryMapper = new DomainEventEntryMapper(builder.schema);
        this.eventSchema = builder.schema;
        this.databaseClient = DatabaseClient.create(builder.connectionFactory);
        this.batchSize = builder.batchSize;
        this.trackedEventDataMapper = new TrackedEventDataMapper(eventSchema, dataType);
        this.upcasterChain = new EventUpcasterChain(NoOpEventUpcaster.INSTANCE);
        this.trackedEventMessageMapper = new TrackedEventMessageMapper(serializer, upcasterChain);
        ReactiveTransactionManager tm = new R2dbcTransactionManager(connectionFactory);
        this.transactionalOperator = TransactionalOperator.create(tm);
        this.domainEventMapper = new DomainEventMapper(serializer, upcasterChain);
        this.persistenceExceptionResolver = builder.persistenceExceptionResolver;
        this.trackedEventsReader = new TrackedEventsReader(
                databaseClient,
                trackedEventDataMapper,
                transactionalOperator
                , builder
        );
        this.appendEventsStatementBuilder = builder.appendEventsStatementBuilder;
        this.appendSnapshotStatementBuilder = builder.appendSnapshotStatementBuilder;
        this.readEventDataForAggregateStatementBuilder = builder.readEventDataForAggregateStatementBuilder;
        this.createHeadTokenStatementBuilder = builder.createHeadTokenStatementBuilder;
        this.createTailTokenStatementBuilder = builder.createTailTokenStatementBuilder;
        this.createTokenAtStatementBuilder = builder.createTokenAtStatementBuilder;
        this.deleteSnapshotsStatementBuilder = builder.deleteSnapshotsStatementBuilder;
        this.lastSequenceNumberForStatementBuilder = builder.lastSequenceNumberForStatementBuilder;
        this.readSnapshotDataStatementBuilder = builder.readSnapshotDataStatementBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }


    @Override
    public Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot) {
        return this.databaseClient
                .inConnectionMany((connection -> {
                    final Statement statement = this.appendSnapshotStatementBuilder.build(connection,
                            this.eventSchema, dataType, snapshot, serializer, null);
                    return Flux.from(statement.execute()).flatMap(result -> result.map((row, rowMetadata) -> ""));
                }))
                .onErrorResume((error -> this.handlePersistenceError(error, snapshot)))
                .onErrorResume(error -> {
                    if (error instanceof ConcurrencyException) {
                        return Mono.empty();
                    } else {
                        return Mono.error(error);
                    }
                })
                .as(this.transactionalOperator::transactional).then();
    }

    private Publisher<? extends String> handlePersistenceError(Throwable error, EventMessage<?> failedEvent) {
        String eventDescription = buildExceptionMessage(failedEvent);
        if (error instanceof Exception && persistenceExceptionResolver != null && persistenceExceptionResolver.isDuplicateKeyViolation((Exception) error)) {
            if (isFirstDomainEvent(failedEvent)) {
                throw new AggregateStreamCreationException(eventDescription, error);
            }
            return Mono.error(new ConcurrencyException(eventDescription, error));
        } else {
            return Mono.error(new EventStoreException(eventDescription, error));
        }
    }

    private String buildExceptionMessage(EventMessage failedEvent) {
        String eventDescription = format("An event with identifier [%s] could not be persisted",
                failedEvent.getIdentifier());
        if (isFirstDomainEvent(failedEvent)) {
            DomainEventMessage failedDomainEvent = (DomainEventMessage) failedEvent;
            eventDescription = format(
                    "Cannot reuse aggregate identifier [%s] to create aggregate [%s] since identifiers need to be unique.",
                    failedDomainEvent.getAggregateIdentifier(),
                    failedDomainEvent.getType());
        } else if (failedEvent instanceof DomainEventMessage<?>) {
            DomainEventMessage failedDomainEvent = (DomainEventMessage) failedEvent;
            eventDescription = format("An event for aggregate [%s] at sequence [%d] was already inserted",
                    failedDomainEvent.getAggregateIdentifier(),
                    failedDomainEvent.getSequenceNumber());
        }
        return eventDescription;
    }

    private boolean isFirstDomainEvent(EventMessage failedEvent) {
        if (failedEvent instanceof DomainEventMessage<?>) {
            return ((DomainEventMessage) failedEvent).getSequenceNumber() == 0L;
        }
        return false;
    }


    @Override
    public Flux<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken) {
        return this
                .readEvents(trackingToken, this.batchSize)
                .map(trackedEventMessageMapper::map);
    }

    @Override
    public Flux<? extends TrackedEventData<?>> readEvents(TrackingToken lastToken, int batchSize) {
        return trackedEventsReader.readEvents(lastToken, batchSize);
    }




    @Override
    public Flux<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        return this.readSnapshotData(aggregateIdentifier).map(domainEventMapper::map);
    }

    @Override
    public Flux<DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        return this.databaseClient
                .inConnectionMany((connection -> {
                    final Statement statement = readSnapshotDataStatementBuilder.build(connection,
                            this.eventSchema, aggregateIdentifier);
                    return Flux.from(statement.execute()).flatMap((r) ->
                            r.map(this.domainEventEntryMapper::map));
                }));
    }

    @Override
    public Mono<Void> appendEvents(List<? extends EventMessage<?>> events) {
        return this.databaseClient
                .inConnectionMany((connection -> {
                    final Statement statement = appendEventsStatementBuilder.build(connection, this.eventSchema, dataType, events,
                            serializer, null);
                    return Flux.from(statement.execute()).flatMap(result -> result.map((row, rowMetadata) -> ""));
                }))
                .onErrorResume((error -> this.handlePersistenceError(error, events.get(0))))
                .as(this.transactionalOperator::transactional).then();
    }


    @Override
    public Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        return this
                .readEvents(aggregateIdentifier, firstSequenceNumber, Integer.MAX_VALUE)
                .map(domainEventMapper::map);
    }


    @Override
    public Flux<? extends DomainEventData<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber,
                                                         int batchSize) {
        return this.databaseClient
                .inConnectionMany(connection -> {
                    final Statement tokenAt =
                            readEventDataForAggregateStatementBuilder.build(connection, eventSchema,
                                    aggregateIdentifier, firstSequenceNumber, batchSize);
                    return Flux
                            .from(tokenAt.execute())
                            .flatMap((r) -> r.map(this.domainEventEntryMapper::map));
                })
                .onErrorResume((error) -> Mono.error(new EventStoreException("Failed to get events", error)))
                .as(this.transactionalOperator::transactional);

    }


    @Override
    public Mono<Optional<Long>> lastSequenceNumberFor(String aggregateIdentifier) {
        return this.databaseClient
                .inConnection(connection -> {
                    final Statement tokenAt = lastSequenceNumberForStatementBuilder.build(connection, eventSchema,
                            aggregateIdentifier);
                    return Flux
                            .from(tokenAt.execute())
                            .flatMap((r) -> r.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Long.class))))
                            .next();
                })
                .onErrorResume((error) -> Mono.error(new EventStoreException("Failed to get tail token", error)))
                .as(this.transactionalOperator::transactional);
    }

    @Override
    public Mono<TrackingToken> createTailToken() {
        return this.databaseClient
                .inConnection(connection -> {
                    final Statement tokenAt = createTailTokenStatementBuilder.build(connection, eventSchema);
                    return Flux
                            .from(tokenAt.execute())
                            .flatMap((r) -> r.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Long.class))))
                            .next();
                }).map(index -> index.map(seq -> GapAwareTrackingToken.newInstance(seq, Collections.emptySet()))
                        .orElse(null))
                .cast(TrackingToken.class)
                .onErrorResume((error) -> Mono.error(new EventStoreException("Failed to get tail token", error)))
                .as(this.transactionalOperator::transactional);

    }

    @Override
    public Mono<TrackingToken> createHeadToken() {
        return this.databaseClient
                .inConnection(connection -> {
                    final Statement tokenAt = createHeadTokenStatementBuilder.build(connection, eventSchema);
                    return Flux
                            .from(tokenAt.execute())
                            .flatMap((r) -> r.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Long.class))))
                            .next();
                })
                .map(index -> index.map(seq -> GapAwareTrackingToken.newInstance(seq, Collections.emptySet()))
                        .orElse(null))
                .cast(TrackingToken.class)
                .onErrorResume((error) -> Mono.error(new EventStoreException("Failed to get  head token", error)))
                .as(this.transactionalOperator::transactional);

    }

    @Override
    public Mono<TrackingToken> createTokenAt(Instant dateTime) {
        return this.databaseClient
                .inConnection(connection -> {
                    final Statement tokenAt = createTokenAtStatementBuilder.build(connection, eventSchema, dateTime);
                    return Flux
                            .from(tokenAt.execute())
                            .flatMap((r) -> r.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Long.class))))
                            .next();
                })
                .map(index -> index.map(seq -> GapAwareTrackingToken.newInstance(seq, Collections.emptySet()))
                        .orElse(null))
                .cast(TrackingToken.class)
                .onErrorResume((error) -> Mono.error(new EventStoreException(format("Failed to get token at [%s]", dateTime), error)))
                .as(this.transactionalOperator::transactional);
    }


    @Override
    public Mono<Void> createSchema() {
        final Mono<Void> eventMono = this.databaseClient
                .inConnectionMany((connection -> {
                    final Statement statement = R2dbcStatementBuilders.createDomainEventTable(connection, this.eventSchema);
                    return Flux.from(statement.execute()).flatMap(result -> result.map((row, rowMetadata) -> ""));
                }))
                .then();
        final Mono<Void> snapshotMono = this.databaseClient.inConnectionMany((connection -> {
            final Statement statement = R2dbcStatementBuilders.createSnapshotEventTable(connection, this.eventSchema);
            return Flux.from(statement.execute()).flatMap(result -> result.map((row, rowMetadata) -> ""));
        })).then();
        return eventMono.zipWith(snapshotMono).then();
    }


    @Override
    public Mono<Void> executeSql(String query) {
        return databaseClient.sql(query).then();
    }

    public static class Builder {

        AppendEventsStatementBuilder appendEventsStatementBuilder = R2dbcStatementBuilders::getAppendEventStatement;
        AppendSnapshotStatementBuilder appendSnapshotStatementBuilder = R2dbcStatementBuilders::appendSnapshot;
        ReadEventDataForAggregateStatementBuilder readEventDataForAggregateStatementBuilder = R2dbcStatementBuilders::readEventsDataForAggregate;
        CleanGapsStatementBuilder cleanGapsStatementBuilder = R2dbcStatementBuilders::cleanGaps;
        CreateHeadTokenStatementBuilder createHeadTokenStatementBuilder = R2dbcStatementBuilders::createHeadToken;
        CreateTailTokenStatementBuilder createTailTokenStatementBuilder = R2dbcStatementBuilders::createTailToken;
        CreateTokenAtStatementBuilder createTokenAtStatementBuilder = R2dbcStatementBuilders::createTokenAt;
        DeleteSnapshotsStatementBuilder deleteSnapshotsStatementBuilder = R2dbcStatementBuilders::deleteSnapshots;
        FetchTrackedEventsStatementBuilder fetchTrackedEventsStatementBuilder = R2dbcStatementBuilders::fetchTrackedEventsStatement;
        LastSequenceNumberForStatementBuilder lastSequenceNumberForStatementBuilder = R2dbcStatementBuilders::lastSequenceNumberFor;
        ReadEventDataWithGapsStatementBuilder readEventDataWithGapsStatementBuilder = R2dbcStatementBuilders::readEventDataWithGaps;
        ReadEventDataWithoutGapsStatementBuilder readEventDataWithoutGapsStatementBuilder = R2dbcStatementBuilders::readEventDataWithoutGaps;
        ReadSnapshotDataStatementBuilder readSnapshotDataStatementBuilder = R2dbcStatementBuilders::readSnapshotData;
        int maxGapOffset = DEFAULT_MAX_GAP_OFFSET;
        long lowestGlobalSequence = DEFAULT_LOWEST_GLOBAL_SEQUENCE;
        int gapTimeout = DEFAULT_GAP_TIMEOUT;
        int gapCleaningThreshold = DEFAULT_GAP_CLEANING_THRESHOLD;
        boolean extendedGapCheckEnabled = DEFAULT_EXTENDED_GAP_CHECK_ENABLED;
        Supplier<Serializer> snapshotSerializer;
        EventUpcaster upcasterChain = NoOpEventUpcaster.INSTANCE;
        PersistenceExceptionResolver persistenceExceptionResolver;
        Supplier<Serializer> eventSerializer = XStreamSerializer::defaultSerializer;
        SnapshotFilter snapshotFilter = SnapshotFilter.allowAll();
        Class<?> dataType = byte[].class;
        EventSchema schema = new EventSchema();
        ConnectionFactory connectionFactory;
        int batchSize;

        /**
         * Set the Statement to be used on {@link R2dbcEventStoreEngine#createTokenAt}. Defaults to {@link
         * R2dbcStatementBuilders#createTokenAt(Connection, EventSchema, Instant)}.
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder createTokenAt(CreateTokenAtStatementBuilder createTokenAt) {
            assertNonNull(createTokenAt, "createTokenAt may not be null");
            this.createTokenAtStatementBuilder = createTokenAt;
            return this;
        }

        public Builder appendEvents(AppendEventsStatementBuilder appendEvents) {
            assertNonNull(appendEvents, "appendEvents may not be null");
            this.appendEventsStatementBuilder = appendEvents;
            return this;
        }

        /**
         * Set the PreparedStatement to be used on {@link R2dbcEventStoreEngine#lastSequenceNumberFor(
         *String)}. Defaults to {@link R2dbcStatementBuilders#lastSequenceNumberFor(Connection, EventSchema,
         * String)}
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder lastSequenceNumberFor(LastSequenceNumberForStatementBuilder lastSequenceNumberFor) {
            assertNonNull(lastSequenceNumberFor, "lastSequenceNumberFor may not be null");
            this.lastSequenceNumberForStatementBuilder = lastSequenceNumberFor;
            return this;
        }

        /**
         * Set the PreparedStatement to be used on {@link R2dbcEventStoreEngine#createTailToken()}. Defaults
         * to {@link R2dbcStatementBuilders#createTailToken(Connection, EventSchema)}
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder createTailToken(CreateTailTokenStatementBuilder createTailToken) {
            assertNonNull(createTailToken, "createTailToken may not be null");
            this.createTailTokenStatementBuilder = createTailToken;
            return this;
        }

        /**
         * Set the PreparedStatement to be used on {@link R2dbcEventStoreEngine#createHeadToken()}. Defaults
         * to {@link R2dbcStatementBuilders#createHeadToken(Connection, EventSchema)}
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder createHeadToken(CreateHeadTokenStatementBuilder createHeadToken) {
            assertNonNull(createHeadToken, "createHeadToken may not be null");
            this.createHeadTokenStatementBuilder = createHeadToken;
            return this;
        }

        public Builder appendSnapshot(AppendSnapshotStatementBuilder appendSnapshot) {
            assertNonNull(appendSnapshot, "appendSnapshot may not be null");
            this.appendSnapshotStatementBuilder = appendSnapshot;
            return this;
        }

        public Builder deleteSnapshots(DeleteSnapshotsStatementBuilder deleteSnapshots) {
            assertNonNull(deleteSnapshots, "deleteSnapshots may not be null");
            this.deleteSnapshotsStatementBuilder = deleteSnapshots;
            return this;
        }

        public Builder fetchTrackedEvents(FetchTrackedEventsStatementBuilder fetchTrackedEvents) {
            assertNonNull(fetchTrackedEvents, "fetchTrackedEvents may not be null");
            this.fetchTrackedEventsStatementBuilder = fetchTrackedEvents;
            return this;
        }

        public Builder cleanGaps(CleanGapsStatementBuilder cleanGaps) {
            assertNonNull(cleanGaps, "cleanGaps may not be null");
            this.cleanGapsStatementBuilder = cleanGaps;
            return this;
        }

        public Builder readEventDataForAggregate(ReadEventDataForAggregateStatementBuilder readEventDataForAggregate) {
            assertNonNull(readEventDataForAggregate, "readEventDataForAggregate may not be null");
            this.readEventDataForAggregateStatementBuilder = readEventDataForAggregate;
            return this;
        }

        public Builder readSnapshotData(ReadSnapshotDataStatementBuilder readSnapshotData) {
            assertNonNull(readSnapshotData, "readSnapshotData may not be null");
            this.readSnapshotDataStatementBuilder = readSnapshotData;
            return this;
        }

        public Builder readEventDataWithoutGaps(ReadEventDataWithoutGapsStatementBuilder readEventDataWithoutGaps) {
            assertNonNull(readEventDataWithoutGaps, "readEventDataWithoutGaps may not be null");
            this.readEventDataWithoutGapsStatementBuilder = readEventDataWithoutGaps;
            return this;
        }

        public Builder readEventDataWithGaps(ReadEventDataWithGapsStatementBuilder readEventDataWithGaps) {
            assertNonNull(readEventDataWithGaps, "readEventDataWithGaps may not be null");
            this.readEventDataWithGapsStatementBuilder = readEventDataWithGaps;
            return this;
        }

        private Builder() {
            persistenceExceptionResolver(new R2dbSqlErrorCodeResolver());
        }


        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            this.snapshotSerializer = () -> snapshotSerializer;
            return this;
        }


        public Builder upcasterChain(EventUpcaster upcasterChain) {
            this.upcasterChain = upcasterChain;
            return this;
        }

        public Builder persistenceExceptionResolver(
                PersistenceExceptionResolver persistenceExceptionResolver
        ) {
            this.persistenceExceptionResolver = persistenceExceptionResolver;
            return this;
        }


        public Builder eventSerializer(Serializer eventSerializer) {
            this.eventSerializer = () -> eventSerializer;
            return this;
        }


        public Builder snapshotFilter(SnapshotFilter snapshotFilter) {
            this.snapshotFilter = snapshotFilter;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the {@link ConnectionProvider} which provides access to a JDBC connection.
         *
         * @param connectionFactory a {@link ConnectionFactory} which provides access to a JDBC connection
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder connectionFactory(ConnectionFactory connectionFactory) {
            assertNonNull(connectionFactory, "connectionFactory may not be null");
            this.connectionFactory = connectionFactory;
            return this;
        }


        /**
         * Sets the {@code dataType} specifying the serialized type of the Event Message's payload and Meta Data.
         * Defaults to the {@code byte[]} {@link Class}.
         *
         * @param dataType a {@link Class} specifying the serialized type of the Event Message's payload and Meta Data
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder dataType(Class<?> dataType) {
            assertNonNull(dataType, "dataType may not be null");
            this.dataType = dataType;
            return this;
        }

        /**
         * Sets the {@link EventSchema} describing the database schema of event entries. Defaults to {@link
         * EventSchema#EventSchema()}.
         *
         * @param schema the {@link EventSchema} describing the database schema of event entries
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder schema(EventSchema schema) {
            assertNonNull(schema, "EventSchema may not be null");
            this.schema = schema;
            return this;
        }

        /**
         * Sets the {@code maxGapOffset} specifying the maximum distance in sequence numbers between a missing event and
         * the event with the highest known index. If the gap is bigger it is assumed that the missing event will not be
         * committed to the store anymore. This event storage engine will no longer look for those events the next time
         * a batch is fetched. Defaults to an integer of {@code 10000} ({@link R2dbcEventStoreEngine#DEFAULT_MAX_GAP_OFFSET}.
         *
         * @param maxGapOffset an {@code int} specifying the maximum distance in sequence numbers between a missing
         *                     event and the event with the highest known index
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder maxGapOffset(int maxGapOffset) {
            assertPositive(maxGapOffset, "maxGapOffset");
            this.maxGapOffset = maxGapOffset;
            return this;
        }

        /**
         * Sets the {@code lowestGlobalSequence} specifying the first expected auto generated sequence number. For most
         * data stores this is 1 unless the table has contained entries before. Defaults to a {@code long} of {@code 1}
         * ({@link R2dbcEventStoreEngine#DEFAULT_LOWEST_GLOBAL_SEQUENCE}).
         *
         * @param lowestGlobalSequence a {@code long} specifying the first expected auto generated sequence number
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder lowestGlobalSequence(long lowestGlobalSequence) {
            assertThat(lowestGlobalSequence,
                    number -> number > 0,
                    "The lowestGlobalSequence must be a positive number");
            this.lowestGlobalSequence = lowestGlobalSequence;
            return this;
        }

        /**
         * Sets the amount of time until a 'gap' in a TrackingToken may be considered timed out. This setting will
         * affect the cleaning process of gaps. Gaps that have timed out will be removed from Tracking Tokens to improve
         * performance of reading events. Defaults to an integer of {@code 60000} ({@link
         * R2dbcEventStoreEngine#DEFAULT_GAP_TIMEOUT}), thus 1 minute.
         *
         * @param gapTimeout an {@code int} specifying the amount of time until a 'gap' in a TrackingToken may be
         *                   considered timed out
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder gapTimeout(int gapTimeout) {
            assertPositive(gapTimeout, "gapTimeout");
            this.gapTimeout = gapTimeout;
            return this;
        }

        /**
         * Sets the threshold of number of gaps in a token before an attempt to clean gaps up is taken. Defaults to an
         * integer of {@code 250} ({@link R2dbcEventStoreEngine#DEFAULT_GAP_CLEANING_THRESHOLD}).
         *
         * @param gapCleaningThreshold an {@code int} specifying the threshold of number of gaps in a token before an
         *                             attempt to clean gaps up is taken
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder gapCleaningThreshold(int gapCleaningThreshold) {
            assertPositive(gapCleaningThreshold, "gapCleaningThreshold");
            this.gapCleaningThreshold = gapCleaningThreshold;
            return this;
        }

        /**
         * Indicates whether an extra query should be performed to verify for gaps in the {@code globalSequence} larger
         * than the configured batch size. These gaps could trick the storage engine into believing there are no more
         * events to read, while there are still positions ahead.
         * <p>
         * This check comes at a cost of an extra query when a batch retrieval yields an empty result. This may increase
         * database pressure when processors are at the HEAD of a stream, as each batch retrieval will result in an
         * extra query, if there are no results.
         * <p>
         * Note that the extra query checks for the smallest globalSequence, higher than the last one seen. This query
         * can be executed using an index, which should be a relatively cheap query for most databases.
         * <p>
         * Defaults to {@code true}
         *
         * @param extendedGapCheckEnabled whether to enable the "extended gap check". Defaults to {@code true}.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder extendedGapCheckEnabled(boolean extendedGapCheckEnabled) {
            this.extendedGapCheckEnabled = extendedGapCheckEnabled;
            return this;
        }

        private void assertPositive(int num, final String numberDescription) {
            assertThat(num, number -> number > 0, "The " + numberDescription + " must be a positive number");
        }

        /**
         * Initializes a {@link R2dbcEventStoreEngine} as specified through this Builder.
         *
         * @return a {@link R2dbcEventStoreEngine} as specified through this Builder
         */
        public R2dbcEventStoreEngine build() {
            return new R2dbcEventStoreEngine(this);
        }


    }

}
