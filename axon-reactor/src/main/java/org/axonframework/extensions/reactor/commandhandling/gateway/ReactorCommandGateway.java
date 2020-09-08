package org.axonframework.extensions.reactor.commandhandling.gateway;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptorSupport;
import org.axonframework.extensions.reactor.messaging.ReactorResultHandlerInterceptorSupport;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Variation of {@link CommandGateway}. Provides support for reactive return type such as {@link Mono} from Project
 * Reactor.
 *
 * @author Milan Savic
 * @since 4.4.2
 */
public interface ReactorCommandGateway extends ReactorMessageDispatchInterceptorSupport<CommandMessage<?>>,
        ReactorResultHandlerInterceptorSupport<CommandMessage<?>, CommandResultMessage<?>> {

    /**
     * Sends the given {@code command} once the caller subscribes to the command result. Returns immediately.
     * <p/>
     * The given {@code command} is wrapped as the payload of a {@link CommandMessage} that is eventually posted on the
     * {@link CommandBus}, unless the {@code command} already implements {@link Message}. In that case, a
     * {@code CommandMessage} is constructed from that message's payload and {@link MetaData}.
     *
     * @param command the command to dispatch
     * @param <R>     the type of the command result
     * @return a {@link Mono} which is resolved when the command is executed
     */
    <R> Mono<R> send(Object command);

    /**
     * Uses given Publisher of commands to send incoming commands away. Commands will be sent sequentially - once a
     * result of Nth command arrives, (N + 1)th command is dispatched.
     *
     * @param commands a Publisher stream of commands to be dispatched
     * @return a Flux of command results. An ordering of command results corresponds to an ordering of commands being
     * dispatched
     *
     * @see #send(Object)
     * @see Flux#concatMap(Function)
     */
    default Flux<Object> sendAll(Publisher<?> commands) {
        return Flux.from(commands)
                   .concatMap(this::send);
    }
}
