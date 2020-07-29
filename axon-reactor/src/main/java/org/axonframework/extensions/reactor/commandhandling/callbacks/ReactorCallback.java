package org.axonframework.extensions.reactor.commandhandling.callbacks;

import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 * Command Handler Callback that allows the dispatching thread to wait for the result of the callback, using the
 * Project Reactor mechanisms. This callback allows the caller to synchronize calls when an asynchronous command bus is
 * being used.
 *
 * @author Stefan Dragisic
 * @since 4.4
 *
 * @see CommandCallback
 * @see org.axonframework.commandhandling.CommandBus
 */
public class ReactorCallback<C, R> extends Mono<CommandResultMessage<? extends R>> implements CommandCallback<C, R> {

    private final EmitterProcessor<CommandResultMessage<? extends R>> commandResultMessageEmitter = EmitterProcessor.create(1);
    private final FluxSink<CommandResultMessage<? extends R>> sink = commandResultMessageEmitter.sink();

    @Override
    public void onResult(CommandMessage<? extends C> commandMessage,
                         CommandResultMessage<? extends R> commandResultMessage) {
        if (commandResultMessage.isExceptional()) {
            sink.error(commandResultMessage.exceptionResult());
        } else {
            sink.next(commandResultMessage);
        }
        sink.complete();
    }

    @Override
    public void subscribe(CoreSubscriber<? super CommandResultMessage<? extends R>> actual) {
        commandResultMessageEmitter.subscribe(actual);
    }

}
