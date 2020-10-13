/*
 * Copyright (c) 2010-2020. Axon Framework
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

package org.axonframework.extensions.reactor.messaging.unitofwork;

import org.axonframework.common.Assert;
import org.axonframework.messaging.GenericResultMessage;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.unitofwork.ExecutionResult;
import org.axonframework.messaging.unitofwork.RollbackConfiguration;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static org.axonframework.messaging.GenericResultMessage.asResultMessage;

/**
 * Implementation of the UnitOfWork that processes a single message.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class DefaultReactiveUnitOfWork<T extends Message<?>> extends AbstractReactiveUnitOfWork<T> {

    private final ReactiveMessageProcessingContext<T> processingContext;

    /**
     * Initializes a Unit of Work (without starting it).
     *
     * @param message the message that will be processed in the context of the unit of work
     */
    public DefaultReactiveUnitOfWork(T message) {
        processingContext = new ReactiveMessageProcessingContext<>(message);
    }

    /**
     * Starts a new DefaultUnitOfWork instance, registering it a CurrentUnitOfWork. This methods returns the started
     * UnitOfWork instance.
     * <p>
     * Note that this Unit Of Work type is not meant to be shared among different Subscriber. A single DefaultUnitOfWork
     * instance should be used exclusively by the Subscriber that created it.
     *
     * @param message the message that will be processed in the context of the unit of work
     * @return the started UnitOfWork instance
     */
    public static <T extends Message<?>> Mono<DefaultReactiveUnitOfWork<T>> startAndGet(T message) {
        DefaultReactiveUnitOfWork<T> uow = new DefaultReactiveUnitOfWork<>(message);
        return uow.start()
                .thenReturn(uow);
    }


    @Override
    public <R> Mono<ResultMessage<R>> executeWithResult(Function<T,Mono<R>> executeTask, RollbackConfiguration rollbackConfiguration) {
        return startIfNotStarted()
                .then(assertAndContinue(phase -> phase == Phase.STARTED, "Executing Default Unit of Work"))
                .then(executeTask.apply(processingContext.getMessage()))
                .map(DefaultReactiveUnitOfWork::toResultMessage)
                .doOnNext(resultMessage -> setExecutionResult(new ExecutionResult(resultMessage)))
                .flatMap(resultMessage -> commit().thenReturn(resultMessage))
                .onErrorResume(t -> recoverAndContinue(rollbackConfiguration, t));
    }

    private Mono<Void> startIfNotStarted() {
      return  Mono.defer(() -> {
            if (phase() == Phase.NOT_STARTED) {
                return start();
            } else {
                return Mono.empty();
            }
        });
    }

    private static <R> ResultMessage<R> toResultMessage(R result) {
        if (result instanceof ResultMessage) {
            return (ResultMessage<R>) result;
        } else if (result instanceof Message) {
            return new GenericResultMessage<>(result, ((Message) result).getMetaData());
        } else {
            return new GenericResultMessage<>(result);
        }
    }

    private <R> Mono<ResultMessage<R>> recoverAndContinue(RollbackConfiguration rollbackConfiguration, Throwable t) {
        return Mono.defer(() -> {
            if (rollbackConfiguration.rollBackOn(t)) {
                return rollback(t).thenReturn(asResultMessage(t));
            }
            return Mono.just(asResultMessage(t));
        });
    }


    @Override
    protected void setRollbackCause(Throwable cause) {
        setExecutionResult(new ExecutionResult(new GenericResultMessage<>(cause)));
    }

    @Override
    protected Mono<Void> notifyHandlers(ReactiveUnitOfWork.Phase phase) {
        return processingContext.notifyHandlers(this, phase);
    }

    @Override
    protected void addHandler(ReactiveUnitOfWork.Phase phase, Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        Assert.state(!phase.isBefore(phase()), () -> "Cannot register a listener for phase: " + phase
                + " because the Unit of Work is already in a later phase: " + phase());
        processingContext.addHandler(phase, handler);
    }

    @Override
    public T getMessage() {
        return processingContext.getMessage();
    }

    @Override
    public ReactiveUnitOfWork<T> transformMessage(Function<T, ? extends Message<?>> transformOperator) {
        processingContext.transformMessage(transformOperator);
        return this;
    }

    @Override
    public ExecutionResult getExecutionResult() {
        return processingContext.getExecutionResult();
    }

    @Override
    protected void setExecutionResult(ExecutionResult executionResult) {
        processingContext.setExecutionResult(executionResult);
    }
}
