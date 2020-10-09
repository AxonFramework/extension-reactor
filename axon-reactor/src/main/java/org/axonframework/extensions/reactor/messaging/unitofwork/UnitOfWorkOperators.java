package org.axonframework.extensions.reactor.messaging.unitofwork;

import reactor.core.publisher.Mono;

/**
 * Helper method that wraps pipeline into single execution context that is shared between Unit of Works
 *
 * @author Stefan Dragisic
 */
public interface UnitOfWorkOperators {

    /**
     * Sets execution for Unit of Work
     * Used when initializing Unit of Work pipeline
     * */
    static <T> Mono<T> executionContext(Mono<T> pipeline) {
        return pipeline.subscriberContext(ReactiveCurrentUnitOfWork.initializeExecutionContext());
    }

    //todo runOnAfterCommitOrNow, runOnPrepareCommitOrNow ... generic operators

}
