package org.axonframework.extensions.reactor.poc.uow;

import reactor.core.publisher.Mono;

/**
 * Helper method that wraps pipeline into single execution context that is shared between Unit of Works
 *
 * @author Stefan Dragisic
 */
public interface UnitOfWorkOperators {

    static <T> Mono<T> executionContext(Mono<T> pipeline) {
        return pipeline.subscriberContext(ReactiveCurrentUnitOfWork.initializeExecutionContext());
    }

}
