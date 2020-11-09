package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.eventhandling.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * @author vtiwar27
 * @date 2020-09-28
 */
public interface ReactiveEventStoreEngine {


    Mono<Void> storeSnapshot(DomainEventMessage<?> snapshot);


    Flux<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken);

    Mono<Void> appendEvents(List<? extends EventMessage<?>> events);

    default Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier) {
        return readEvents(aggregateIdentifier, 0);
    }

    Flux<DomainEventMessage<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber);

    Flux<? extends TrackedEventData<?>> readEvents(TrackingToken trackingToken, int batchSize);

    Flux<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier);

    Flux<DomainEventData<?>> readSnapshotData(String aggregateIdentifier);


    Flux<? extends DomainEventData<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber,
                                                  int batchSize);

    Mono<Optional<Long>> lastSequenceNumberFor(String aggregateIdentifier);

    Mono<TrackingToken> createTailToken();

    Mono<TrackingToken> createHeadToken();

    Mono<TrackingToken> createTokenAt(Instant dateTime);

    Mono<Void> createSchema();

    Mono<Void> executeSql(String query);
}
