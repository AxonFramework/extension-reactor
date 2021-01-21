package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author vtiwar27
 * @date 2020-11-10
 */
public interface BlockingReactiveEventStoreEngineSupport extends ReactiveEventStoreEngine {

    Flux<? extends DomainEventData<?>> readEvents(String aggregateIdentifier, long firstSequenceNumber,
                                                  int batchSize);

    Flux<? extends TrackedEventData<?>> readEvents(TrackingToken trackingToken, int batchSize);

    Mono<Void> createSchema();

    Mono<Void> executeSql(String query);
}
