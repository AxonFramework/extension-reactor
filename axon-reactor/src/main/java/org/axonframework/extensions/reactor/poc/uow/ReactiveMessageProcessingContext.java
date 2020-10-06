package org.axonframework.extensions.reactor.poc.uow;

import org.axonframework.common.Assert;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.unitofwork.ExecutionResult;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * Maintains the context around the processing of a single Message. This class notifies handlers when the Unit of Work
 * processing the Message transitions to a new {@link ReactiveUnitOfWork.Phase}.
 *
 * @author Stefan Dragisic
 */
public class ReactiveMessageProcessingContext<T extends Message<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMessageProcessingContext.class);
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final Deque EMPTY = new LinkedList<>();

    private final EnumMap<ReactiveUnitOfWork.Phase, Deque<Function<Mono<ReactiveUnitOfWork<T>>, Mono<Void>>>> handlers = new EnumMap<>(ReactiveUnitOfWork.Phase.class);
    private T message;
    private ExecutionResult executionResult;

    /**
     * Creates a new processing context for the given {@code message}.
     *
     * @param message The Message that is to be processed.
     */
    public ReactiveMessageProcessingContext(T message) {
        this.message = message;
    }

    /**
     * Invoke the handlers in this collection attached to the given {@code phase}.
     *
     * @param unitOfWork The Unit of Work that is changing its phase
     * @param phase      The phase for which attached handlers should be invoked
     */
    @SuppressWarnings("unchecked")
    public Mono<Void> notifyHandlers(ReactiveUnitOfWork<T> unitOfWork, ReactiveUnitOfWork.Phase phase) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Notifying handlers for phase {}", phase.toString());
        }
        Mono<ReactiveUnitOfWork<T>> uowMono = Mono.just(unitOfWork);

        return Mono.fromCallable(() ->
                (Deque<Function<Mono<ReactiveUnitOfWork<T>>, Mono<Void>>>) handlers.getOrDefault(phase, EMPTY))
                .flatMapIterable(Function.identity())
                .concatMap(handler -> handler.apply(uowMono))
                .then();
    }

    /**
     * Adds a handler to the collection. Note that the order in which you register the handlers determines the order
     * in which they will be handled during the various stages of a unit of work.
     *
     * @param phase   The phase of the unit of work to attach the handler to
     * @param handler The handler to invoke in the given phase
     */
    public void addHandler(ReactiveUnitOfWork.Phase phase, Function<Mono<ReactiveUnitOfWork<T>>, Mono<Void>> handler) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Adding handler {} for phase {}", handler.getClass().getName(), phase.toString());
        }
        final Deque<Function<Mono<ReactiveUnitOfWork<T>>, Mono<Void>>> consumers = handlers.computeIfAbsent(phase, p -> new ArrayDeque<>());
        if (phase.isReverseCallbackOrder()) {
            consumers.addFirst(handler);
        } else {
            consumers.add(handler);
        }
    }

    /**
     * Get the Message that is being processed in this context.
     *
     * @return the Message that is being processed
     */
    public T getMessage() {
        return message;
    }

    /**
     * Get the result of processing the {@link #getMessage() Message}. If the Message has not been processed yet this
     * method returns {@code null}.
     *
     * @return The result of processing the Message, or {@code null} if the Message hasn't been processed
     */
    public ExecutionResult getExecutionResult() {
        return executionResult;
    }

    /**
     * Set the execution result of processing the current {@link #getMessage() Message}. In case this context has a
     * previously set ExecutionResult, setting a new result is only allowed if the new result is an exception result.
     * <p/>
     * In case the previously set result is also an exception result, the exception in the new execution result is
     * added to the original exception as a suppressed exception.
     *
     * @param executionResult the ExecutionResult of the currently handled Message
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void setExecutionResult(ExecutionResult executionResult) {
        Assert.state(this.executionResult == null || executionResult.isExceptionResult(),
                () -> String.format("Cannot change execution result [%s] to [%s] for message [%s].",
                        message, this.executionResult, executionResult));
        if (this.executionResult != null && this.executionResult.isExceptionResult()) {
            this.executionResult.getExceptionResult().addSuppressed(executionResult.getExceptionResult());
        } else {
            this.executionResult = executionResult;
        }
    }

    /**
     * Transform the Message being processed using the given operator.
     *
     * @param transformOperator The transform operator to apply to the stored message
     */
    public void transformMessage(Function<T, ? extends Message<?>> transformOperator) {
        message = (T) transformOperator.apply(message);
    }

    /**
     * Reset the processing context. This clears the execution result and map with registered handlers, and replaces
     * the current Message with the given {@code message}.
     *
     * @param message The new message that is being processed
     */
    public void reset(T message) {
        this.message = message;
        handlers.clear();
        executionResult = null;
    }
}
