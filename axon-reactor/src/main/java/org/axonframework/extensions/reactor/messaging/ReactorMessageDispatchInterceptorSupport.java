package org.axonframework.extensions.reactor.messaging;

import org.axonframework.common.Registration;
import org.axonframework.messaging.Message;

/**
 * Interface marking components capable of registering a {@link ReactorMessageDispatchInterceptor}. Generally, these
 * are messaging components injected into the sending end of the communication.
 *
 * @param <M> The type of the message to be intercepted
 * @author Milan Savic
 * @since 4.4
 */
public interface ReactorMessageDispatchInterceptorSupport<M extends Message<?>> {

    /**
     * Register the given {@link ReactorMessageDispatchInterceptor}. After registration, the interceptor will be
     * invoked for each message dispatched on the messaging component that it was registered to.
     *
     * @param interceptor The reactive interceptor to register
     * @return a Registration, which may be used to unregister the interceptor
     */
    Registration registerDispatchInterceptor(ReactorMessageDispatchInterceptor<M> interceptor);
}
