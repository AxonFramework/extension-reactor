/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.reactor.commandhandling;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.GenericCommandResultMessage;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;

import java.util.LinkedList;

/**
 * A stub of {@link CommandBus} that captures sent commands.
 *
 * @author Sara Pellegrini
 * @since 4.4.2
 */
public class CommandBusStub implements CommandBus {

    private final LinkedList<CommandMessage<?>> sent = new LinkedList<>();

    private final CommandResultMessage result;

    public CommandBusStub() {
        this(new GenericCommandResultMessage<Object>(""));
    }

    public CommandBusStub(CommandResultMessage<?> result) {
        this.result = result;
    }

    @Override
    public <C> void dispatch(CommandMessage<C> command) {
        sent.add(command);
    }

    @Override
    public <C, R> void dispatch(CommandMessage<C> command, CommandCallback<? super C, ? super R> callback) {
        sent.add(command);
        callback.onResult(command, result);
    }

    @Override
    public Registration subscribe(String commandName, MessageHandler<? super CommandMessage<?>> handler) {
        return null;
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        return null;
    }

    @Override
    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<? super CommandMessage<?>> handlerInterceptor) {
        return null;
    }

    public CommandMessage<?> lastSentCommand() {
        return sent.getLast();
    }

    public int numberOfSentCommands() {
        return sent.size();
    }
}
