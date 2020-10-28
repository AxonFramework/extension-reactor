package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.ConnectionFactory;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventEntryMapper;
import org.axonframework.extensions.reactor.eventstore.mappers.DomainEventMapper;
import org.axonframework.extensions.reactor.eventstore.statements.R2dbcStatementBuilders;
import org.axonframework.serialization.Serializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

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


    public R2dbcEventStoreEngine(ConnectionFactory connectionFactory,
                                 EventSchema eventSchema,
                                 Class<?> dataType, Serializer serializer) {
        this.connectionFactory = connectionFactory;
        this.dataType = dataType;
        this.serializer = serializer;
        this.domainEventEntryMapper = new DomainEventEntryMapper(eventSchema);
        this.eventSchema = eventSchema;
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
        return Mono.from(connectionFactory.create()).flatMap(c -> Mono.from(c.beginTransaction()).then(
                Mono.from(R2dbcStatementBuilders.getAppendEventStatement(c, events, this.eventSchema, serializer, dataType)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .doFinally((st) -> c.close()))).then();
    }


    @Override
    public Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        return Flux.from(connectionFactory.create()).flatMap(c -> Flux.from(c.beginTransaction()).thenMany(
                Flux.from(R2dbcStatementBuilders.getEventsStatement(c, eventSchema, 100, aggregateIdentifier, firstSequenceNumber)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .flatMap(r -> r.map(this.domainEventEntryMapper::map))
                        .map(DomainEventMapper::map)
                        .doFinally((st) -> c.close())));
    }


    @Override
    public Mono<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        return Mono.from(connectionFactory.create()).flatMap(c ->
                Mono.from(c.beginTransaction()).then(
                        Mono.from(R2dbcStatementBuilders.lastSequenceNumberFor(c, this.eventSchema, aggregateIdentifier)
                                .execute())
                                .delayUntil(r -> c.commitTransaction())
                                .map(r -> r.map(((row, rowMetadata) -> row.get(0, Long.class))))
                                .flatMap(Mono::from)
                                .onErrorResume((error) ->
                                        Mono.error(new EventStoreException(format("Failed to read events for aggregate [%s]", aggregateIdentifier), error)))
                                .doFinally((st) -> c.close())));

    }

    @Override
    public Mono<TrackingToken> createTailToken() {
        return Mono.from(connectionFactory.create()).flatMap(c -> Mono.from(c.beginTransaction()).then(
                Mono.from(R2dbcStatementBuilders.createTailToken(c, this.eventSchema)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .map(r -> r.map(((row, rowMetadata) -> row.get(0, Long.class))))
                        .flatMap(Mono::from)
                        .map(index -> Optional.ofNullable(index)
                                .map(seq -> GapAwareTrackingToken.
                                        newInstance(seq, Collections.emptySet()))
                                .orElse(null))
                        .onErrorResume((error) ->
                                Mono.error(new EventStoreException("Failed to get tail token", error)))
                        .doFinally((st) -> c.close())));

    }

    @Override
    public Mono<TrackingToken> createHeadToken() {
        return Mono.from(connectionFactory.create()).flatMap(c -> Mono.from(c.beginTransaction()).then(
                Mono.from(R2dbcStatementBuilders.createHeadToken(c, this.eventSchema)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .map(r -> r.map(((row, rowMetadata) -> row.get(0, Long.class))))
                        .flatMap(Mono::from)
                        .map(index -> Optional.ofNullable(index)
                                .map(seq -> GapAwareTrackingToken.
                                        newInstance(seq, Collections.emptySet()))
                                .orElse(null))
                        .onErrorResume((error) ->
                                Mono.error(new EventStoreException("Failed to get head token", error)))
                        .doFinally((st) -> c.close())));

    }

    @Override
    public Mono<TrackingToken> createTokenAt(Instant dateTime) {
        return Mono.from(connectionFactory.create()).flatMap(c -> Mono.from(c.beginTransaction()).then(
                Mono.from(R2dbcStatementBuilders.createTokenAt(c, this.eventSchema, dateTime)
                        .execute())
                        .delayUntil(r -> c.commitTransaction())
                        .map(r -> r.map(((row, rowMetadata) -> row.get(0, Long.class))))
                        .flatMap(Mono::from)
                        .map(index -> Optional.ofNullable(index)
                                .map(seq -> GapAwareTrackingToken.
                                        newInstance(seq, Collections.emptySet()))
                                .orElse(null))
                        .onErrorResume((error) ->
                                Mono.error(new EventStoreException(format("Failed to get token at [%s]", dateTime), error)))
                        .doFinally((st) -> c.close())));
    }


}
