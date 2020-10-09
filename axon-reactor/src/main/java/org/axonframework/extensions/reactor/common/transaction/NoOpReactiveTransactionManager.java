package org.axonframework.extensions.reactor.common.transaction;

import reactor.core.publisher.Mono;

/**
 * TransactionManager implementation that does nothing. It's a placeholder implementation for the cases where no
 * special transaction management is required.
 *
 * @author Stefan Dragisic
 */
enum NoTransactionManager implements ReactiveAxonTransactionManager {

    /**
     * Singleton instance of the TransactionManager
     */
    INSTANCE;

    /**
     * Returns the singleton instance of this TransactionManager
     *
     * @return the singleton instance of this TransactionManager
     */
    public static ReactiveAxonTransactionManager instance() {
        return INSTANCE;
    }

    @Override
    public Mono<ReactiveAxonTransaction> startTransaction() {
        return Mono.just(TRANSACTION);
    }

    private static final ReactiveAxonTransaction TRANSACTION = new ReactiveAxonTransaction() {
        @Override
        public Mono<Void> commit() {
            //no op
            return Mono.empty();
        }

        @Override
        public Mono<Void> rollback() {
            //no op
            return Mono.empty();
        }
    };
}