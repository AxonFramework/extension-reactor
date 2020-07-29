package org.axonframework.extensions.reactor.messaging;

import org.axonframework.messaging.Message;
import org.axonframework.messaging.ResultMessage;
import reactor.core.publisher.Flux;

/**
 * Interceptor that allows results to be intercepted and modified before they are handled. Implementations are required
 * to operate on a {@link Flux} of results or return a new {@link Flux} which will be passed down the interceptor chain.
 * Also, implementations may make decisions based on the message that was dispatched.
 *
 * @param <M> The type of the message for which the result is going to be intercepted
 * @param <R> The type of the result to be intercepted
 * @author Sara Pellegrini
 * @since 4.4
 */
@FunctionalInterface
public interface ReactorResultHandlerInterceptor<M extends Message<?>, R extends ResultMessage<?>> {

    /**
     * Intercepts result messages. It's possible to break the interceptor chain by returning {@link Flux#empty()} or
     * {@link Flux#error(Throwable)} variations.
     *
     * @param message a message that was dispatched (and caused these {@code results})
     * @param results the outcome of the dispatched {@code message}
     * @return the intercepted {@code results}
     */
    Flux<R> intercept(M message, Flux<R> results);
}
