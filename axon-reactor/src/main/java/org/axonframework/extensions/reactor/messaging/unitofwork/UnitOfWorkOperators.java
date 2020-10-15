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

    /**
     * Executes this Mono onPrepareCommit phase or now
     *
     * Used to wrap operations that needs to be executed onPrepareCommit phase
     * or now if current unit of work is not started
     * */
    static Mono<Void> onPrepareCommitOrNow(Mono<Void> task) {
        return ReactiveCurrentUnitOfWork.ifStartedRun(uow -> uow.onPrepareCommit(u -> task))
                .flatMap(executed-> executed ? Mono.empty() : task);
    }

    /**
     * Executes this Mono onAfterCommit phase or now
     *
     * Used to wrap operations that needs to be executed onAfterCommit phase
     * or now if current unit of work is not started */
    static Mono<Void> onAfterCommitOrNow(Mono<Void> task) {
        return ReactiveCurrentUnitOfWork.ifStartedRun(uow -> uow.afterCommit(u -> task))
                .flatMap(executed-> executed ? Mono.empty() : task);
    }

}
