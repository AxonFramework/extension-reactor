package org.axonframework.extensions.reactor.poc.uow;


import reactor.core.publisher.Mono;

/**
 * TODO DOC
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
