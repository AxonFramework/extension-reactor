package org.axonframework.extensions.reactor.eventhandling.gateway;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptor;
import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptorSupport;
import org.axonframework.messaging.Message;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;

/**
 * Variation of the {@link EventGateway}, wrapping a {@link EventBus} for a friendlier API. Provides support for
 * reactive return types such as {@link Flux} from Project
 * Reactor.
 *
 * @author Milan Savic
 * @since 4.4
 */
public interface ReactorEventGateway extends ReactorMessageDispatchInterceptorSupport<EventMessage<?>> {

    /**
     * Publishes given {@code events} once the caller subscribes to the resulting Flux. Returns immediately.
     * <p/>
     * Given {@code events} are wrapped as payloads of a {@link EventMessage} that are eventually published on the
     * {@link EventBus}, unless {@code event} already implements {@link Message}. In that case, a {@code EventMessage}
     * is constructed from that message's payload and {@link org.axonframework.messaging.MetaData}.
     *
     * @param events events to be published
     * @return events that were published. DO NOTE: if there were some {@link ReactorMessageDispatchInterceptor}s
     * registered to this {@code gateway}, they will be processed first, before returning events to the caller. The
     * order of returned events is the same as one provided as the input parameter.
     */
    default Flux<Object> publish(Object... events) { // NOSONAR
        return publish(Arrays.asList(events));
    }

    /**
     * Publishes the given {@code events} once the caller subscribes to the resulting {@link Flux}. Returns immediately.
     * <p/>
     * Given {@code events} are wrapped as payloads of a {@link EventMessage} that are eventually published on the
     * {@link EventBus}, unless {@code event} already implements {@link Message}. In that case, a {@code EventMessage}
     * is constructed from that message's payload and {@link org.axonframework.messaging.MetaData}.
     *
     * @param events the list of events to be published
     * @return events that were published. DO NOTE: if there were some {@link ReactorMessageDispatchInterceptor}s
     * registered to this {@code gateway}, they will be processed first, before returning events to the caller. The
     * order of returned events is the same as one provided as the input parameter.
     */
    Flux<Object> publish(List<?> events);

    /**
     * Publishes the given {@code events} once the caller subscribes to the resulting {@link Flux}. Returns immediately.
     * <p/>
     * Given {@code events} are wrapped as payloads of a {@link EventMessage} that are eventually published on the
     * {@link EventBus}, unless {@code event} already implements {@link Message}. In that case, a {@code EventMessage}
     * is constructed from that message's payload and {@link org.axonframework.messaging.MetaData}.
     *
     * @param events the publisher of events to be published
     * @return events that were published. DO NOTE: if there were some {@link ReactorMessageDispatchInterceptor}s
     * registered to this {@code gateway}, they will be processed first, before returning events to the caller. The
     * order of returned events is the same as one provided as the input parameter.
     */
    default Flux<Object> publishAll(Publisher<?> events) {
        return Flux.from(events)
                   .concatMap(this::publish);
    }
}
