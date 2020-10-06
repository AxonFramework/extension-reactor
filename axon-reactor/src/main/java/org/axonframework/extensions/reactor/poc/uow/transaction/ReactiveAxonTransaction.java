package org.axonframework.extensions.reactor.poc.uow.transaction;


import reactor.core.publisher.Mono;

/**
 * Interface of an object that represents a started transaction that can be committed or rolled back.
 *
 * @author Stefan Dragisic
 */
public interface ReactiveAxonTransaction {

    /**
     * Commit this transaction.
     */
    Mono<Void> commit();

    /**
     * Roll back this transaction.
     */
    Mono<Void> rollback();

}
