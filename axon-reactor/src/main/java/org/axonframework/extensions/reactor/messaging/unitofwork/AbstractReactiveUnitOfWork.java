package org.axonframework.extensions.reactor.messaging.unitofwork;

import org.axonframework.common.Assert;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.correlation.CorrelationDataProvider;
import org.axonframework.messaging.unitofwork.AbstractUnitOfWork;
import org.axonframework.messaging.unitofwork.ExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Abstract implementation of the Reactor Unit of Work. It provides default implementations of all methods related to the
 * processing of a Message.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public abstract class AbstractReactiveUnitOfWork<T extends Message<?>> implements ReactiveUnitOfWork<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractUnitOfWork.class);
    private final Map<String, Object> resources = new HashMap<>();
    private final Collection<CorrelationDataProvider> correlationDataProviders = new LinkedHashSet<>();
    private ReactiveUnitOfWork<?> parentUnitOfWork;
    private ReactiveUnitOfWork.Phase phase = ReactiveUnitOfWork.Phase.NOT_STARTED;
    private boolean rolledBack;

    @Override
    public Mono<Void> start() {
        return Mono.fromRunnable(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting Unit Of Work");
            }
            Assert.state(ReactiveUnitOfWork.Phase.NOT_STARTED.equals(phase()), () -> "UnitOfWork is already started");
            rolledBack = false;
            onRollbackRun(u -> rolledBack = true);
        })
                .then(ReactiveCurrentUnitOfWork.ifStarted(parent ->
                                Mono.fromRunnable(() -> {
                                    // we're nesting.
                                    this.parentUnitOfWork = parent;
                                    root().onCleanup(r -> changePhase(Phase.CLEANUP, Phase.CLOSED));
                                })
                ))
                .then(changePhase(ReactiveUnitOfWork.Phase.STARTED))
                .and(ReactiveCurrentUnitOfWork.set(this));
    }

    @Override
    public Mono<Void> commit() {
        return Mono.fromRunnable(() -> {//todo refactor
            if (logger.isDebugEnabled()) {
                logger.debug("Committing Unit Of Work");
            }
            Assert.state(phase() == ReactiveUnitOfWork.Phase.STARTED, () -> String.format("The UnitOfWork is in an incompatible phase: %s", phase()));
        }).then(isCurrent())
                .flatMap(current -> !current ? Mono.error(new RuntimeException("The UnitOfWork is not the current Unit of Work")) : isRoot() ? commitAsRoot() : commitAsNested())
                .then(ReactiveCurrentUnitOfWork.clear(this))
                .onErrorResume(t -> ReactiveCurrentUnitOfWork.clear(this).and(Mono.error(t)));
    }

    private Mono<Void> commitAsRoot() {
        return changePhase(ReactiveUnitOfWork.Phase.PREPARE_COMMIT, ReactiveUnitOfWork.Phase.COMMIT)
                .onErrorResume(t -> {
                    setRollbackCause(t);
                    return changePhase(
                            ReactiveUnitOfWork.Phase.ROLLBACK)
                            .and(Mono.error(t));
                })
                .then(Mono.defer(()-> {
                    if (phase() == Phase.COMMIT) {
                        return changePhase(Phase.AFTER_COMMIT);
                    } else return Mono.empty();
                }))
                .then(changePhase(ReactiveUnitOfWork.Phase.CLEANUP, ReactiveUnitOfWork.Phase.CLOSED))
                .onErrorResume(t ->
                        changePhase(Phase.CLEANUP, Phase.CLOSED)
                        .and(Mono.error(t)));
    }

    private Mono<Void> commitAsNested() {
        return changePhase(ReactiveUnitOfWork.Phase.PREPARE_COMMIT, ReactiveUnitOfWork.Phase.COMMIT)
                .then(delegateAfterCommitToParent(this))
                .and(Mono.fromRunnable(() -> parentUnitOfWork.onRollback(u -> changePhase(Phase.ROLLBACK))))
                .onErrorResume(t -> {
                    setRollbackCause(t);
                    return changePhase(ReactiveUnitOfWork.Phase.ROLLBACK)
                            .then(Mono.error(t));
                });
    }


    private Mono<Void> delegateAfterCommitToParent(ReactiveUnitOfWork<?> reactiveUnitOfWorkMono) {
        return Mono.defer(()->{
            Optional<ReactiveUnitOfWork<?>> parent = reactiveUnitOfWorkMono.parent();
            if (parent.isPresent()) {
                parent.get().afterCommit(this::delegateAfterCommitToParent);
                return Mono.empty();
            } else {
                return changePhase(ReactiveUnitOfWork.Phase.AFTER_COMMIT);
            }
        });
    }

    @Override
    public Mono<Void> rollback(Throwable cause) {
        return Mono.fromRunnable(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Rolling back Unit Of Work.", cause);
            }
            Assert.state(isActive() && phase().isBefore(ReactiveUnitOfWork.Phase.ROLLBACK),
                    () -> String.format("The UnitOfWork is in an incompatible phase: %s", phase()));
        }).then(isCurrent()
                .flatMap(current -> {
                    if (!current) {
                        return Mono.error(new IllegalStateException("The UnitOfWork is not the current Unit of Work"));
                    } else {
                        setRollbackCause(cause);
                        if (isRoot()) {
                            return changePhase(ReactiveUnitOfWork.Phase.ROLLBACK)
                                    .and(changePhase(ReactiveUnitOfWork.Phase.CLEANUP, ReactiveUnitOfWork.Phase.CLOSED));
                        }
                        return changePhase(ReactiveUnitOfWork.Phase.ROLLBACK);
                    }
                }))
                .then(ReactiveCurrentUnitOfWork.clear(this))
                .onErrorResume(t -> ReactiveCurrentUnitOfWork.clear(this).and(Mono.error(t)));
    }

    @Override
    public Optional<ReactiveUnitOfWork<?>> parent() {
        return Optional.ofNullable(parentUnitOfWork);
    }


    @Override
    public Map<String, Object> resources() {
        return resources;
    }

    @Override
    public boolean isRolledBack() {
        return rolledBack;
    }

    @Override
    public void registerCorrelationDataProvider(CorrelationDataProvider correlationDataProvider) {
        correlationDataProviders.add(correlationDataProvider);
    }

    @Override
    public ReactiveUnitOfWork.Phase phase() {
        return phase;
    }

    @Override
    public MetaData getCorrelationData() {
        if (correlationDataProviders.isEmpty()) {
            return MetaData.emptyInstance();
        }
        Map<String, Object> result = new HashMap<>();
        for (CorrelationDataProvider correlationDataProvider : correlationDataProviders) {
            final Map<String, ?> extraData = correlationDataProvider.correlationDataFor(getMessage());
            if (extraData != null) {
                result.putAll(extraData);
            }
        }
        return MetaData.from(result);
    }


    @Override
    public void onPrepareCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        addHandler(ReactiveUnitOfWork.Phase.PREPARE_COMMIT, handler);
    }

    @Override
    public void onCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        addHandler(ReactiveUnitOfWork.Phase.COMMIT, handler);
    }

    @Override
    public void afterCommit(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        addHandler(Phase.AFTER_COMMIT, handler);
    }

    @Override
    public void onRollback(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        addHandler(Phase.ROLLBACK, handler);
    }

    @Override
    public void onCleanup(Function<ReactiveUnitOfWork<T>, Mono<Void>> handler) {
        addHandler(Phase.CLEANUP, handler);
    }

    protected abstract void addHandler(ReactiveUnitOfWork.Phase phase, Function<ReactiveUnitOfWork<T>, Mono<Void>> handler);

    /**
     * Overwrite the current phase with the given {@code phase}.
     *
     * @param phase the new phase of the Unit of Work
     */
    protected void setPhase(ReactiveUnitOfWork.Phase phase) {
        this.phase = phase;
    }

    /**
     * Ask the unit of work to transition to the given {@code phases} sequentially. In each of the phases the
     * unit of work is responsible for invoking the handlers attached to each phase.
     * <p/>
     * By default this sets the Phase and invokes the handlers attached to the phase.
     *
     * @param phases The phases to transition to in sequential order
     */
    protected Mono<Void> changePhase(ReactiveUnitOfWork.Phase... phases) {
        return Flux.fromArray(phases)
                .doOnNext(this::setPhase)
                .concatMap(this::notifyHandlers)
                .then();
    }

    /**
     * Provides the collection of registered Correlation Data Providers of this Unit of Work. The returned collection is a live view of the providers
     * registered. Any changes in the registration are reflected in the returned collection.
     *
     * @return The Correlation Data Providers registered with this Unit of Work.
     */
    protected Collection<CorrelationDataProvider> correlationDataProviders() {
        return correlationDataProviders;
    }

    /**
     * Notify the handlers attached to the given {@code phase}.
     *
     * @param phase The phase for which to invoke registered handlers.
     */
    protected abstract Mono<Void> notifyHandlers(ReactiveUnitOfWork.Phase phase);

    /**
     * Set the execution result of processing the current {@link #getMessage() Message}.
     *
     * @param executionResult the ExecutionResult of the currently handled Message
     */
    protected abstract void setExecutionResult(ExecutionResult executionResult);

    /**
     * Sets the cause for rolling back this Unit of Work.
     *
     * @param cause The cause for rolling back this Unit of Work
     */
    protected abstract void setRollbackCause(Throwable cause);

}


