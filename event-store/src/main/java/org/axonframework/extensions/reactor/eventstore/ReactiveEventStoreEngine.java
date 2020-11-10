package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.eventhandling.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * @author vtiwar27
 * @date 2020-09-28
 */
public interface ReactiveEventStoreEngine {


    Mono<Void> appendEvents(List<? extends EventMessage<?>> events);

    default Mono<Void> appendEvents(EventMessage<?>... events) {
        return appendEvents(asList(events));
    }

    default Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier) {
        return readEvents(aggregateIdentifier, 0);
    }

    Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot);


    Flux<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken);


    Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber);


    Flux<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier);

    Flux<DomainEventData<?>> readSnapshotData(String aggregateIdentifier);


    Mono<Optional<Long>> lastSequenceNumberFor(String aggregateIdentifier);

    Mono<TrackingToken> createTailToken();

    Mono<TrackingToken> createHeadToken();

    Mono<TrackingToken> createTokenAt(Instant dateTime);


}
