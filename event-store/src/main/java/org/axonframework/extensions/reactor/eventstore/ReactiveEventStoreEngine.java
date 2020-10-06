package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackingToken;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * @author vtiwar27
 * @date 2020-09-28
 */
public interface ReactiveEventStoreEngine {
    Mono<Void> appendEvents(EventMessage<?>... events);

    Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot);


    Mono<Void> appendEvents(Flux<? extends EventMessage<?>> events);

    default Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier) {
        return readEvents(aggregateIdentifier, 0);
    }

    Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber);

    Mono<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier);

    Mono<Long> lastSequenceNumberFor(String aggregateIdentifier);

    Mono<TrackingToken> createTailToken();

    Mono<TrackingToken> createHeadToken();

    Mono<TrackingToken> createTokenAt(Instant dateTime);

}
