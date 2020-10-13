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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.axonframework.messaging.GenericResultMessage.asResultMessage;

/**
 * Unit of Work implementation that is able to process a batch of Messages instead of just a single Message.
 *
 * @param <T> The type of message handled by this Unit of Work
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class BatchingReactiveUnitOfWork<T extends Message<?>> extends AbstractReactiveUnitOfWork<T> {

    private final List<ReactiveMessageProcessingContext<T>> processingContexts;
    Throwable cause = null;
    private ReactiveMessageProcessingContext<T> processingContext;

    /**
     * Initializes a BatchingReactiveUnitOfWork for processing the given batch of {@code messages}.
     *
     * @param messages batch of messages to process
     */
    @SafeVarargs
    public BatchingReactiveUnitOfWork(T... messages) {
        this(Arrays.asList(messages));
    }

    /**
     * Initializes a BatchingUnitOfWork for processing the given list of {@code messages}.
     *
     * @param messages batch of messages to process
     */
    public BatchingReactiveUnitOfWork(List<T> messages) {
        Assert.isFalse(messages.isEmpty(), () -> "The list of Messages to process is empty");
        processingContexts = messages.stream().map(ReactiveMessageProcessingContext::new).collect(Collectors.toList());
        processingContext = processingContexts.get(0);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p/>
     * This implementation executes the given {@code task} for each of its messages. The return value is the
     * result of the last executed task.
     */
    @Override
    public <R> Mono<ResultMessage<R>> executeWithResult(Function<T,Mono<R>> task, RollbackConfiguration rollbackConfiguration) {
        return startIfNotStarted()
                .then(assertAndContinue(phase -> phase == Phase.STARTED, "Executing Batching with Result"))
                .thenMany(Flux.defer(() -> Flux.fromIterable(processingContexts)))
                .doOnNext(context -> processingContext = context)
                .concatMap(r -> executeTask(r.getMessage(), task, rollbackConfiguration))
                .doOnNext(resultMessage -> setExecutionResult(new ExecutionResult(resultMessage)))
                .last()
                .flatMap(resultMessage -> commit().thenReturn(resultMessage))
                .onErrorResume(t -> Mono.just(asResultMessage(t)))
                .log();
    }

    private <R> Mono<ResultMessage<R>> executeTask(T message, Function<T,Mono<R>> task, RollbackConfiguration rollbackConfiguration) {
        return task.apply(message)
                .map(this::toResultMessage)
                .onErrorResume(t -> recoverAndContinue(t, rollbackConfiguration));
    }

    private Mono<Void> startIfNotStarted(){
        return Mono.defer(() -> {
            cause = null;
            return phase() == Phase.NOT_STARTED ? start() : Mono.empty();
        });
    }

    private <R> Mono<ResultMessage<R>> recoverAndContinue(Throwable t, RollbackConfiguration rollbackConfiguration) {
        return Mono.defer(() -> {
            if (rollbackConfiguration.rollBackOn(t)) {
                return rollback(t).thenReturn(asResultMessage(t));
            }

            if (cause != null) {
                cause.addSuppressed(t);
            } else {
                cause = t;
            }
            return Mono.just(asResultMessage(cause));

        });
    }

    private <R> ResultMessage<R> toResultMessage(R result) {
        if (result instanceof ResultMessage) {
            return (ResultMessage<R>) result;
        } else if (result instanceof Message) {
            return new GenericResultMessage<>(result, ((Message) result).getMetaData());
        } else {
            return new GenericResultMessage<>(result);
        }
    }

    /**
     * Returns a Map of {@link ExecutionResult} per Message. If the Unit of Work has not been given a task
     * to execute, the ExecutionResult is {@code null} for each Message.
     *
     * @return a Map of ExecutionResult per Message processed by this Unit of Work
     */
    public Map<Message<?>, ExecutionResult> getExecutionResults() {
        return processingContexts.stream().collect(
                Collectors.toMap(ReactiveMessageProcessingContext::getMessage, ReactiveMessageProcessingContext::getExecutionResult));
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

    @Override
    protected Mono<Void> notifyHandlers(Phase phase) {
        return Flux.defer(() -> {
            Iterator<ReactiveMessageProcessingContext<T>> iterator =
                    phase.isReverseCallbackOrder() ? new LinkedList<>(processingContexts).descendingIterator() :
                            processingContexts.iterator();

            return Flux.<ReactiveMessageProcessingContext<T>>push(fluxSink -> {
                while (iterator.hasNext()) {
                    fluxSink.next(iterator.next());
                }
                fluxSink.complete();
            })
                    .concatMap(context -> (processingContext = context).notifyHandlers(this, phase));
        }).then();
    }

    @Override
    protected void setRollbackCause(Throwable cause) {
        processingContexts.forEach(context -> context
                .setExecutionResult(new ExecutionResult(new GenericResultMessage<>(cause))));
    }

    @Override
    protected void addHandler(ReactiveUnitOfWork.Phase phase, Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        processingContext.addHandler(phase, handler);
    }

    /**
     * Get the batch of messages that is being processed (or has been processed) by this unit of work.
     *
     * @return the message batch
     */
    public List<? extends T> getMessages() {
        return processingContexts.stream().map(ReactiveMessageProcessingContext::getMessage).collect(Collectors.toList());
    }

    /**
     * Checks if the given {@code message} is the last of the batch being processed in this unit of work.
     *
     * @param message the message to check for
     * @return {@code true} if the message is the last of this batch, {@code false} otherwise
     */
    public boolean isLastMessage(Message<?> message) {
        return processingContexts.get(processingContexts.size() - 1).getMessage().equals(message);
    }

    /**
     * Checks if the message being processed now is the last of the batch being processed in this unit of work.
     *
     * @return {@code true} if the message is the last of this batch, {@code false} otherwise
     */
    public boolean isLastMessage() {
        return isLastMessage(getMessage());
    }

    /**
     * Checks if the given {@code message} is the first of the batch being processed in this unit of work.
     *
     * @param message the message to check
     * @return {@code true} if the message is the first of this batch, {@code false} otherwise
     */
    public boolean isFirstMessage(Message<?> message) {
        return processingContexts.get(0).getMessage().equals(message);
    }

    /**
     * Checks if the message being processed now is the first of the batch being processed in this unit of work.
     *
     * @return {@code true} if the message is the first of this batch, {@code false} otherwise
     */

    public boolean isFirstMessage() {
        return isFirstMessage(getMessage());
    }

}
