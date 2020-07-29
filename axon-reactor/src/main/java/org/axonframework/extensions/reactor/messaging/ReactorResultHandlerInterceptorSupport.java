package org.axonframework.extensions.reactor.messaging;

import org.axonframework.common.Registration;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.ResultMessage;

/**
 * Interface marking components capable of registering a {@link ReactorResultHandlerInterceptor}. Generally, these are
 * messaging components injected into the receiving end of the communication.
 *
 * @param <M> The type of the message for which the result is going to be intercepted
 * @param <R> The type of the result to be intercepted
 * @author Milan Savic
 * @since 4.4
 */
public interface ReactorResultHandlerInterceptorSupport<M extends Message<?>, R extends ResultMessage<?>> {

    /**
     * Register the given {@link ReactorResultHandlerInterceptor}. After registration, the interceptor will be invoked
     * for each result message received on the messaging component that it was registered to.
     *
     * @param interceptor The reactive interceptor to register
     * @return a Registration, which may be used to unregister the interceptor
     */
    Registration registerResultHandlerInterceptor(ReactorResultHandlerInterceptor<M, R> interceptor);
}
