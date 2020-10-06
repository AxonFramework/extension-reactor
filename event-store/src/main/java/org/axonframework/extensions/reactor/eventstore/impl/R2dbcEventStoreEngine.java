package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.Connection;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * @author vtiwar27
 * @date 2020-09-30
 */


public class R2dbcEventStoreEngine implements ReactiveEventStoreEngine {


    @Override
    public Mono<Void> appendEvents(EventMessage<?>... events) {

        return null;
    }

    @Override
    public Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot) {
        return null;
    }

    @Override
    public Mono<Void> appendEvents(List<? extends EventMessage<?>> events) {
        return null;
    }

    @Override
    public Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        return null;
    }

    @Override
    public Mono<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        return null;
    }

    @Override
    public Mono<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        return null;
    }

    @Override
    public Mono<TrackingToken> createTailToken() {
        return null;
    }

    @Override
    public Mono<TrackingToken> createHeadToken() {
        return null;
    }

    @Override
    public Mono<TrackingToken> createTokenAt(Instant dateTime) {
        return null;
    }


}
