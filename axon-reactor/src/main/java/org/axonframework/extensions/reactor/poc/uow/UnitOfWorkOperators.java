package org.axonframework.extensions.reactor.poc.uow;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * TODO DOC
 * @author Stefan Dragisic
 */
public interface UnitOfWorkOperators {

    static <T> Mono<T> executionContext(Mono<T> pipeline) {
        return pipeline.subscriberContext(ReactiveCurrentUnitOfWork.initializeTransactionContext());
    }

}
