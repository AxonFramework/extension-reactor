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
import java.util.function.Function;
import java.util.function.Predicate;

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
        return assertAndContinue(
                phase -> phase == Phase.NOT_STARTED,
                () -> {
                    rolledBack = false;
                    onRollbackRun(u -> rolledBack = true);
                }, "Starting Unit Of Work")
                .then(ReactiveCurrentUnitOfWork.ifStartedRun(parent -> {
                            this.parentUnitOfWork = parent;
                            root().onCleanup(r -> changePhase(Phase.CLEANUP, Phase.CLOSED));
                        }
                ))
                .then(changePhase(ReactiveUnitOfWork.Phase.STARTED))
                .and(ReactiveCurrentUnitOfWork.set(this));
    }

    @Override
    public Mono<Void> commit() {
        return assertAndContinue(phase -> phase == Phase.STARTED, "Committing Unit Of Work")
                .then(isCurrent())
                .flatMap(this::commitIfCurrent)
                .then(ReactiveCurrentUnitOfWork.clear(this))
                .onErrorResume(t -> ReactiveCurrentUnitOfWork.clear(this).and(Mono.error(t)));
    }

    private Mono<Void> commitIfCurrent(Boolean current) {
        return current ? isRoot() ? commitAsRoot() : commitAsNested() : Mono.error(new RuntimeException("The UnitOfWork is not the current Unit of Work"));
    }

    private Mono<Void> commitAsRoot() {
        return changePhase(ReactiveUnitOfWork.Phase.PREPARE_COMMIT, ReactiveUnitOfWork.Phase.COMMIT)
                .onErrorResume(this::rollbackAndPropagateError)
                .then(changePhaseIfInCommitPhase())
                .then(changePhase(ReactiveUnitOfWork.Phase.CLEANUP, ReactiveUnitOfWork.Phase.CLOSED))
                .onErrorResume(this::cleanUpAndPropagateError);
    }

    private Mono<Void> commitAsNested() {
        return changePhase(ReactiveUnitOfWork.Phase.PREPARE_COMMIT, ReactiveUnitOfWork.Phase.COMMIT)
                .then(delegateAfterCommitToParent(this))
                .and(setParentRollbackHook())
                .onErrorResume(this::rollbackAndPropagateError);
    }

    private Mono<Void> delegateAfterCommitToParent(ReactiveUnitOfWork<?> reactiveUnitOfWorkMono) {
        return Mono.defer(() -> {
            Optional<ReactiveUnitOfWork<?>> parent = reactiveUnitOfWorkMono.parent();
            if (parent.isPresent()) {
                parent.get().afterCommit(this::delegateAfterCommitToParent);
                return Mono.empty();
            } else {
                return changePhase(ReactiveUnitOfWork.Phase.AFTER_COMMIT);
            }
        });
    }

    private Mono<Void> changePhaseIfInCommitPhase() {
        return Mono.defer(() -> {
            if (phase() == Phase.COMMIT) {
                return changePhase(Phase.AFTER_COMMIT);
            } else return Mono.empty();
        });
    }

    private Mono<Object> setParentRollbackHook() {
        return Mono.fromRunnable(() -> parentUnitOfWork.onRollback(u -> changePhase(Phase.ROLLBACK)));
    }

    @Override
    public Mono<Void> rollback(Throwable cause) {
        return assertAndContinue(
                phase -> isActive() && phase.isBefore(Phase.ROLLBACK), "Rolling back Unit Of Work. Error: " + cause.getLocalizedMessage()
        ).then(isCurrent()
                .flatMap(current -> rollbackIfCurrent(current, cause)))
                .then(ReactiveCurrentUnitOfWork.clear(this))
                .onErrorResume(t -> ReactiveCurrentUnitOfWork.clear(this).and(Mono.error(t)));
    }

    private Mono<Void> rollbackIfCurrent(boolean current, Throwable cause) {
        if (current) {
            setRollbackCause(cause);
            return isRoot() ? changePhase(Phase.ROLLBACK)
                    .and(changePhase(Phase.CLEANUP, Phase.CLOSED)) : changePhase(Phase.ROLLBACK);
        } else {
            return Mono.error(new IllegalStateException("The UnitOfWork is not the current Unit of Work"));
        }
    }

    private Mono<Void> cleanUpAndPropagateError(Throwable t) {
        return changePhase(Phase.CLEANUP, Phase.CLOSED)
                .and(Mono.error(t));
    }

    private Mono<Void> rollbackAndPropagateError(Throwable t) {
        return Mono.defer(() -> {
            setRollbackCause(t);
            return changePhase(
                    ReactiveUnitOfWork.Phase.ROLLBACK)
                    .and(Mono.error(t));
        });
    }

    protected Mono<Void> assertAndContinue(Predicate<Phase> predicate, String logMessage) {
        return assertAndContinue(predicate, null, logMessage);

    }

    protected Mono<Void> assertAndContinue(Predicate<Phase> predicate, Runnable doAfterAssert, String logMessage) {
        return Mono.fromRunnable(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug(logMessage);
            }
            Assert.state(predicate.test(phase()), () -> String.format("The UnitOfWork is in an incompatible phase: %s", phase()));
            if (doAfterAssert != null) doAfterAssert.run();
        });
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
                .concatMap(phase->{
                    setPhase(phase);
                    return notifyHandlers(phase);
                })
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


