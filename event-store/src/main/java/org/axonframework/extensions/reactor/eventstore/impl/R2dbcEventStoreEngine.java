package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventEntryMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventMapper;
import org.axonframework.extensions.reactor.eventstore.statements.R2dbcStatementBuilders;
import org.axonframework.serialization.Serializer;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.axonframework.common.DateTimeUtils.formatInstant;

/**
 * @author vtiwar27
 * @date 2020-09-30
 */


public class R2dbcEventStoreEngine implements ReactiveEventStoreEngine {

    private final ConnectionFactory connectionFactory;
    private final EventSchema eventSchema;
    private final Class<?> dataType;
    private final Serializer serializer;
    private final DomainEventEntryMapper domainEventEntryMapper;
    private final DatabaseClient databaseClient;
    private final int batchSize = 1;

    public R2dbcEventStoreEngine(ConnectionFactory connectionFactory,
                                 EventSchema eventSchema,
                                 Class<?> dataType, Serializer serializer) {
        this.connectionFactory = connectionFactory;
        this.dataType = dataType;
        this.serializer = serializer;
        this.domainEventEntryMapper = new DomainEventEntryMapper(eventSchema);
        this.eventSchema = eventSchema;
        this.databaseClient = DatabaseClient.create(connectionFactory);

    }


    @Override
    public Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot) {
        return Mono.error(new UnsupportedOperationException());
    }

    @Override
    public Flux<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken) {
        return Flux.error(new UnsupportedOperationException());
    }

    @Override
    public Mono<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        return Mono.error(new UnsupportedOperationException());
    }

    @Override
    public Mono<Void> appendEvents(List<? extends EventMessage<?>> events) {
        return this.databaseClient.inConnectionMany((connection -> {
            final Statement statement = R2dbcStatementBuilders.getAppendEventStatement(connection, events,
                    this.eventSchema, serializer, dataType);
            return Flux.from(statement.execute()).flatMap(result -> result.map((row, rowMetadata) -> ""));
        })).then();
    }


    @Override
    public Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber) {

        return this.databaseClient
                .sql(R2dbcStatementBuilders.getEventsStatement(eventSchema))
                .bind("$1", aggregateIdentifier)
                .bind("$2", firstSequenceNumber)
                .bind("$3", (firstSequenceNumber + this.batchSize))
                .map(this.domainEventEntryMapper::map)
                .all()
                .map(DomainEventMapper::map);
    }


    @Override
    public Mono<Optional<Long>> lastSequenceNumberFor(String aggregateIdentifier) {
        return this.databaseClient.sql(R2dbcStatementBuilders.lastSequenceNumberFor(eventSchema)).bind("$1", aggregateIdentifier)
                .map((res, metadata) -> Optional.ofNullable(res.get(0, Long.class))).first();


    }

    @Override
    public Mono<TrackingToken> createTailToken() {
        return this.databaseClient.sql(R2dbcStatementBuilders.createTailToken(eventSchema))
                .map((res, metadata) -> Optional.ofNullable(res.get(0, Long.class))).first().map(index -> index.map(seq -> GapAwareTrackingToken.
                        newInstance(seq, Collections.emptySet())).orElse(null)).cast(TrackingToken.class).onErrorResume((error) ->
                        Mono.error(new EventStoreException("Failed to get tail token", error)));
    }

    @Override
    public Mono<TrackingToken> createHeadToken() {
        return this.databaseClient.sql(R2dbcStatementBuilders.createHeadToken(eventSchema))
                .map((res, metadata) -> Optional.ofNullable(res.get(0, Long.class))).first().map(index -> index.map(seq -> GapAwareTrackingToken.
                        newInstance(seq, Collections.emptySet())).orElse(null)).cast(TrackingToken.class).onErrorResume((error) ->
                        Mono.error(new EventStoreException("Failed to get head token", error)));

    }

    @Override
    public Mono<TrackingToken> createTokenAt(Instant dateTime) {
        return this.databaseClient.sql(R2dbcStatementBuilders.createTokenAt(eventSchema, dateTime)).bind("$1", formatInstant(dateTime))
                .map((res, metadata) -> Optional.ofNullable(res.get(0, Long.class))).first().map(index -> index.map(seq -> GapAwareTrackingToken.
                        newInstance(seq, Collections.emptySet())).orElse(null)).cast(TrackingToken.class).onErrorResume((error) ->
                        Mono.error(new EventStoreException(format("Failed to get token at [%s]", dateTime), error)));
    }


    public Mono<Void> createSchema() {
        return Mono.from(connectionFactory.create()).flatMap(c -> Mono.from(c.beginTransaction()).then(
                Mono.from(R2dbcStatementBuilders.createDomainEventTable(c, this.eventSchema)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .doFinally((st) -> c.close()))).then();
    }

}
