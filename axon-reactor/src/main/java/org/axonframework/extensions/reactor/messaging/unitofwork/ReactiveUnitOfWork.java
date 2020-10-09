package org.axonframework.extensions.reactor.messaging.unitofwork;

import org.axonframework.extensions.reactor.common.transaction.ReactiveAxonTransactionManager;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.correlation.CorrelationDataProvider;
import org.axonframework.messaging.unitofwork.*;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class represents a Unit of Work that monitors the processing of a {@link Message}.
 * <p/>
 * Before processing begins a Unit of Work is bound to the active subscription by registering it with the {@link
 * ReactiveCurrentUnitOfWork}. After processing, the Unit of Work is unregistered from the {@link ReactiveCurrentUnitOfWork}.
 * <p/>
 * Handlers can be notified about the state of the processing of the Message by registering with this Unit of Work.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public interface ReactiveUnitOfWork<T extends Message<?>> {

    /**
     * Starts the current unit of work. The UnitOfWork instance is registered with the CurrentUnitOfWork.
     */
    Mono<Void> start();

    /**
     * Commits the Unit of Work. This should be invoked after the Unit of Work Message has been processed. Handlers
     * registered to the Unit of Work will be notified.
     * <p/>
     * After the commit (successful or not), any registered clean-up handlers will be
     * invoked and the Unit of Work is unregistered from the {@link CurrentUnitOfWork}.
     * <p/>
     * If the Unit of Work fails to commit, e.g. because an exception is raised by one of its handlers, the Unit of Work
     * is rolled back.
     *
     * @throws IllegalStateException if the UnitOfWork wasn't started or if the Unit of Work is not the 'current' Unit
     *                               of Work returned by {@link CurrentUnitOfWork#get()}.
     */
    Mono<Void> commit();

    /**
     * Initiates the rollback of this Unit of Work, invoking all registered rollback and
     * clean-up handlers  respectively. Finally, the Unit of Work is unregistered from the
     * {@link CurrentUnitOfWork}.
     * <p/>
     * If the rollback is a result of an exception, consider using {@link #rollback(Throwable)} instead.
     *
     * @throws IllegalStateException if the Unit of Work is not in a compatible phase.
     */
    default Mono<Void> rollback() {
        return rollback(new RuntimeException("Unknown cause"));
    }


    /**
     * Initiates the rollback of this Unit of Work, invoking all registered rollback respectively.
     * Finally, the Unit of Work is unregistered from the
     * {@link CurrentUnitOfWork}.
     *
     * @param cause The cause of the rollback. May be {@code null}.
     * @throws IllegalStateException if the Unit of Work is not in a compatible phase.
     */
    Mono<Void> rollback(Throwable cause);

    /**
     * Indicates whether this UnitOfWork is started. It is started when the {@link #start()} method has been called, and
     * if the UnitOfWork has not been committed or rolled back.
     *
     * @return {@code true} if this UnitOfWork is started, {@code false} otherwise.
     */
    default boolean isActive() {
        return phase().isStarted();
    }

    /**
     * Returns the current phase of the Unit of Work.
     *
     * @return the Unit of Work phase
     */
    ReactiveUnitOfWork.Phase phase();

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#PREPARE_COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    void onPrepareCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#PREPARE_COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    default void onPrepareCommitRun(Consumer<ReactiveUnitOfWork<T>> handler) {
        onPrepareCommit(u->Mono.fromRunnable(() -> handler.accept(u)));
    }

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    void onCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    default void onCommitRun(Consumer<ReactiveUnitOfWork<T>> handler) {
        onCommit(u->Mono.fromRunnable(() -> handler.accept(u)));
    }

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link UnitOfWork.Phase#AFTER_COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    void afterCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link UnitOfWork.Phase#AFTER_COMMIT}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    default void afterCommitRun(Consumer<ReactiveUnitOfWork<T>> handler) {
        afterCommit(u->Mono.fromRunnable(() -> handler.accept(u)));
    }

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#ROLLBACK}. On rollback, the cause for the rollback can obtained from the
     * supplied
     *
     * @param handler the handler to register with the Unit of Work
     */
    void onRollback(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);


    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#ROLLBACK}. On rollback, the cause for the rollback can obtained from the
     * supplied
     *
     * @param handler the handler to register with the Unit of Work
     */
    default void onRollbackRun(Consumer<ReactiveUnitOfWork<T>> handler) {
        onRollback(u->Mono.fromRunnable(()-> handler.accept(u)));
    }

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#CLEANUP}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    void onCleanup(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);

    /**
     * Register given {@code handler} with the Unit of Work. The handler will be notified when the phase of the
     * Unit of Work changes to {@link ReactiveUnitOfWork.Phase#CLEANUP}.
     *
     * @param handler the handler to register with the Unit of Work
     */
    default void onCleanupRun(Consumer<ReactiveUnitOfWork<T>> handler) {
        onCleanup(u->Mono.fromRunnable(()-> handler.accept(u)));
    }

    /**
     * Returns an optional for the parent of this Unit of Work. The optional holds the Unit of Work that was active when
     * this Unit of Work was started. In case no other Unit of Work was active when this Unit of Work was started the
     * optional is empty, indicating that this is the Unit of Work root.
     *
     * @return an optional parent Unit of Work
     */
    Optional<ReactiveUnitOfWork<?>> parent();

    /**
     * Check that returns {@code true} if this Unit of Work has not got a parent.
     *
     * @return {@code true} if this Unit of Work has no parent
     */
    default boolean isRoot() {
        return !parent().isPresent();
    }

    /**
     * Returns the root of this Unit of Work. If this Unit of Work has no parent (see {@link #parent()}) it returns
     * itself, otherwise it returns the root of its parent.
     *
     * @return the root of this Unit of Work
     */
    default ReactiveUnitOfWork<?> root() {
        //noinspection unchecked // cast is used to remove inspection error in IDE
        return parent().map(ReactiveUnitOfWork::root).orElse((ReactiveUnitOfWork) this);
    }

    /**
     * Get the message that is being processed by the Unit of Work. A Unit of Work processes a single Message over its
     * life cycle.
     *
     * @return the Message being processed by this Unit of Work
     */
    T getMessage();

    /**
     * Transform the Message being processed using the given operator and stores the result.
     * <p>
     * Implementations should take caution not to change the message type to a type incompatible with the current Unit
     * of Work. For example, do not return a CommandMessage when transforming an EventMessage.
     *
     * @param transformOperator The transform operator to apply to the stored message
     * @return this Unit of Work
     */
    ReactiveUnitOfWork<T> transformMessage(Function<T, ? extends Message<?>> transformOperator);

    /**
     * Get the correlation data contained in the {@link #getMessage() message} being processed by the Unit of Work.
     * <p/>
     * By default this correlation data will be copied to other {@link Message messages} created in the context of this
     * Unit of Work, so long as these messages extend from {@link org.axonframework.messaging.GenericMessage}.
     *
     * @return The correlation data contained in the message processed by this Unit of Work
     */
    MetaData getCorrelationData();

    /**
     * Register given {@code correlationDataProvider} with this Unit of Work. Correlation data providers are used
     * to provide meta data based on this Unit of Work's {@link #getMessage() Message} when {@link
     * #getCorrelationData()} is invoked.
     *
     * @param correlationDataProvider the Correlation Data Provider to register
     */
    void registerCorrelationDataProvider(CorrelationDataProvider correlationDataProvider);

    /**
     * Returns a mutable map of resources registered with the Unit of Work.
     *
     * @return mapping of resources registered with this Unit of Work
     */
    Map<String, Object> resources();

    /**
     * Returns the resource attached under given {@code name}, or {@code null} if no such resource is
     * available.
     *
     * @param name The name under which the resource was attached
     * @param <R>  The type of resource
     * @return The resource mapped to the given {@code name}, or {@code null} if no resource was found.
     */
    @SuppressWarnings("unchecked")
    default <R> R getResource(String name) {
        return (R) resources().get(name);
    }

    /**
     * Returns the resource attached under given {@code name}. If there is no resource mapped to the given key yet
     * the {@code mappingFunction} is invoked to provide the mapping.
     *
     * @param key             The name under which the resource was attached
     * @param mappingFunction The function that provides the mapping if there is no mapped resource yet
     * @param <R>             The type of resource
     * @return The resource mapped to the given {@code key}, or the resource returned by the
     * {@code mappingFunction} if no resource was found.
     */
    @SuppressWarnings("unchecked")
    default <R> R getOrComputeResource(String key, Function<? super String, R> mappingFunction) {
        return (R) resources().computeIfAbsent(key, mappingFunction);
    }

    /**
     * Returns the resource attached under given {@code name}. If there is no resource mapped to the given key,
     * the {@code defaultValue} is returned.
     *
     * @param key          The name under which the resource was attached
     * @param defaultValue The value to return if no mapping is available
     * @param <R>          The type of resource
     * @return The resource mapped to the given {@code key}, or the resource returned by the
     * {@code mappingFunction} if no resource was found.
     */
    @SuppressWarnings("unchecked")
    default <R> R getOrDefaultResource(String key, R defaultValue) {
        return (R) resources().getOrDefault(key, defaultValue);
    }

    /**
     * Attach a transaction to this Unit of Work, using the given {@code transactionManager}. The transaction will be
     * managed in the lifecycle of this Unit of Work. Failure to start a transaction will cause this Unit of Work
     * to be rolled back.
     *
     * @param transactionManager The Transaction Manager to create, commit and/or rollback the transaction
     */
    default Mono<Void> attachTransaction(ReactiveAxonTransactionManager transactionManager) {
        return transactionManager.startTransaction()
                .doOnNext(transaction -> {
                    onCommit(u -> transaction.commit());
                    onRollback(u -> transaction.rollback());
                })
                .onErrorResume(t-> rollback(t).then(Mono.error(t)))
                .then();
    }

    /**
     * Execute the given {@code task} in the context of this Unit of Work. If the Unit of Work is not started yet
     * it will be started.
     * <p/>
     * If the task executes successfully the Unit of Work is committed. If any exception is raised while executing the
     * task, the Unit of Work is rolled back and the exception is thrown.
     *
     * @param task the task to execute
     */
    default Mono<Void> execute(Mono<Void> task) {
        return execute(task, RollbackConfigurationType.ANY_THROWABLE);
    }

    /**
     * Execute the given {@code task} in the context of this Unit of Work. If the Unit of Work is not started yet
     * it will be started.
     * <p/>
     * If the task executes successfully the Unit of Work is committed. If an exception is raised while executing the
     * task, the {@code rollbackConfiguration} determines if the Unit of Work should be rolled back or committed,
     * and the exception is thrown.
     *
     * @param task                  the task to execute
     * @param rollbackConfiguration configucsration that determines whether or not to rollback the unit of work when task
     *                              execution fails
     */
    default Mono<Void> execute(Mono<Void> task, RollbackConfiguration rollbackConfiguration) {
        return executeWithResult(task, rollbackConfiguration)
                .flatMap(result -> result.isExceptional()
                        ? Mono.error(new RuntimeException(result.exceptionResult())) :
                        Mono.empty())
                .then();
    }

    /**
     * Execute the given {@code task} in the context of this Unit of Work. If the Unit of Work is not started yet
     * it will be started.
     * <p/>
     * If the task executes successfully the Unit of Work is committed and the result of the task is returned. If any
     * exception is raised while executing the task, the Unit of Work is rolled back and the exception is thrown.
     *
     * @param <R>  the type of result that is returned after successful execution
     * @param task the task to execute
     * @return The result of the task wrapped in Result Message
     */
    default <R> Mono<ResultMessage<R>> executeWithResult(Mono<R> task) {
        return executeWithResult(task, RollbackConfigurationType.ANY_THROWABLE);
    }

    /**
     * Execute the given {@code task} in the context of this Unit of Work. If the Unit of Work is not started yet
     * it will be started.
     * <p/>
     * If the task executes successfully the Unit of Work is committed and the result of the task is returned. If
     * execution fails, the {@code rollbackConfiguration} determines if the Unit of Work should be rolled back or
     * committed.
     *
     * @param <R>                   the type of result that is returned after successful execution
     * @param task                  the task to execute
     * @param rollbackConfiguration configuration that determines whether or not to rollback the unit of work when task
     *                              execution fails
     * @return The result of the task wrapped in Result Message
     */
    <R> Mono<ResultMessage<R>> executeWithResult(Mono<R> task, RollbackConfiguration rollbackConfiguration);

    /**
     * Get the result of the task that was executed by this Unit of Work. If the Unit of Work has not been given a task
     * to execute this method returns {@code null}.
     * <p>
     * Note that the value of the returned ExecutionResult's {@link ExecutionResult#isExceptionResult()} does not
     * determine whether or not the UnitOfWork has been rolled back. To check whether or not the UnitOfWork was rolled
     * back check {@link #isRolledBack}.
     *
     * @return The result of the task executed by this Unit of Work, or {@code null} if the Unit of Work has not
     * been given a task to execute.
     */
    ExecutionResult getExecutionResult();

    /**
     * Check if the Unit of Work has been rolled back.
     *
     * @return {@code true} if the unit of work was rolled back, {@code false} otherwise.
     */
    boolean isRolledBack();

    /**
     * Check if the Unit of Work is the 'currently' active Unit of Work returned by {@link CurrentUnitOfWork#get()}.
     *
     * @return {@code true} if the Unit of Work is the currently active Unit of Work
     */
    default Mono<Boolean> isCurrent() {
        return ReactiveCurrentUnitOfWork.isStarted()//todo ifStartedGet
                .zipWith(ReactiveCurrentUnitOfWork.get())
                .map(tuple -> tuple.getT1() && tuple.getT2() == this);
    }

    /**
     * Enum indicating possible phases of the Unit of Work.
     */
    enum Phase {

        /**
         * Indicates that the unit of work has been created but has not been registered with the {@link
         * CurrentUnitOfWork} yet.
         */
        NOT_STARTED(false, false),

        /**
         * Indicates that the Unit of Work has been registered with the {@link CurrentUnitOfWork} but has not been
         * committed, because its Message has not been processed yet.
         */
        STARTED(true, false),

        /**
         * Indicates that the Unit of Work is preparing its commit. This means that {@link #commit()} has been invoked
         * on the Unit of Work, indicating that the Message {@link #getMessage()} of the Unit of Work has been
         * processed.
         * <p/>
         * All handlers registered to be notified before commit {@link #onPrepareCommit} will be invoked. If no
         * exception is raised by any of the handlers the Unit of Work will go into the {@link #COMMIT} phase, otherwise
         * it will be rolled back.
         */
        PREPARE_COMMIT(true, false),

        /**
         * Indicates that the Unit of Work has been committed and is passed the {@link #PREPARE_COMMIT} phase.
         */
        COMMIT(true, true),

        /**
         * Indicates that the Unit of Work is being rolled back. Generally this is because an exception was raised while
         * processing the {@link #getMessage() message} or while the Unit of Work was being committed.
         */
        ROLLBACK(true, true),

        /**
         * Indicates that the Unit of Work is after a successful commit. In this phase the Unit of Work cannot be rolled
         * back anymore.
         */
        AFTER_COMMIT(true, true),

        /**
         * Indicates that the Unit of Work is after a successful commit or after a rollback. Any resources tied to this
         * Unit of Work should be released.
         */
        CLEANUP(false, true),

        /**
         * Indicates that the Unit of Work is at the end of its life cycle. This phase is final.
         */
        CLOSED(false, true);

        private final boolean started;
        private final boolean reverseCallbackOrder;

        Phase(boolean started, boolean reverseCallbackOrder) {
            this.started = started;
            this.reverseCallbackOrder = reverseCallbackOrder;
        }

        /**
         * Check if a Unit of Work in this phase has been started, i.e. is registered with the {@link
         * CurrentUnitOfWork}.
         *
         * @return {@code true} if the Unit of Work is started when in this phase, {@code false} otherwise
         */
        public Boolean isStarted() {
            return started;
        }

        /**
         * Check whether registered handlers for this phase should be invoked in the order of registration (first
         * registered handler is invoked first) or in the reverse order of registration (last registered handler is
         * invoked first).
         *
         * @return {@code true} if the order of invoking handlers in this phase should be in the reverse order of
         * registration, {@code false} otherwise.
         */
        public boolean isReverseCallbackOrder() {
            return reverseCallbackOrder;
        }

        /**
         * Check if this Phase comes before given other {@code phase}.
         *
         * @param phase The other Phase
         * @return {@code true} if this comes before the given {@code phase}, {@code false} otherwise.
         */
        public boolean isBefore(ReactiveUnitOfWork.Phase phase) {
            return ordinal() < phase.ordinal();
        }

        /**
         * Check if this Phase comes after given other {@code phase}.
         *
         * @param phase The other Phase
         * @return {@code true} if this comes after the given {@code phase}, {@code false} otherwise.
         */
        public boolean isAfter(ReactiveUnitOfWork.Phase phase) {
            return ordinal() > phase.ordinal();
        }
    }

}
