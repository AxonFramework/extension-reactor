package org.axonframework.extensions.reactor.messaging.unitofwork;

import org.axonframework.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Nested Reactive Unit Of Work
 *
 * @author Stefan Dragisic
 */
public class NestedReactiveUnitOfWorkTest {

    private List<PhaseTransition> phaseTransitions = new ArrayList<>();
    private DefaultReactiveUnitOfWork<GenericEventMessage<String>> outer;
    private DefaultReactiveUnitOfWork<GenericEventMessage<String>> middle;
    private DefaultReactiveUnitOfWork<GenericEventMessage<String>> inner;

    @SuppressWarnings({"unchecked"})
    @BeforeEach
    void setUp() {
        Hooks.onOperatorDebug();
        Hooks.enableContextLossTracking();

        outer = new DefaultReactiveUnitOfWork(new GenericEventMessage<>("outer"));
        middle = new DefaultReactiveUnitOfWork(new GenericEventMessage<>("middle"));
        inner = new DefaultReactiveUnitOfWork(new GenericEventMessage<>("inner"));

        outer = registerListeners(outer);
        middle = registerListeners(middle);
        inner = registerListeners(inner);
    }

    private DefaultReactiveUnitOfWork<GenericEventMessage<String>> registerListeners(DefaultReactiveUnitOfWork<GenericEventMessage<String>> unitOfWork) {
        unitOfWork.onPrepareCommitRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.PREPARE_COMMIT)));
        unitOfWork.onCommitRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.COMMIT)));
        unitOfWork.afterCommitRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.AFTER_COMMIT)));
        unitOfWork.onRollbackRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.ROLLBACK)));
        unitOfWork.onCleanupRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.CLEANUP)));

        return unitOfWork;
    }


    @Test
    void testInnerUnitOfWorkNotifiedOfOuterCommitFailure() {

        outer.onPrepareCommit(u -> Mono.when(inner.start()).then(inner.commit()));
        outer.onCommit(u -> Mono.error(MockException::new));
        outer.onCommitRun(u -> phaseTransitions.add(new PhaseTransition(u, ReactiveUnitOfWork.Phase.COMMIT, "x")));

    outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectError(MockException.class)
                .verify();

        assertEquals(Arrays.asList(new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.COMMIT, "x"),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }


    @Test
    void testInnerUnitOfWorkNotifiedOfOuterPrepareCommitFailure() {
        outer.onPrepareCommit(u -> Mono.when(inner.start()).then(inner.commit()));
        outer.onPrepareCommit(u -> Mono.error(MockException::new));

        outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectError(MockException.class)
                .verify();

        assertEquals(Arrays.asList(new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void testInnerUnitOfWorkNotifiedOfOuterCommit() {
        outer.onPrepareCommit(u -> Mono.when(inner.start()).then(inner.commit()));

        outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectComplete()
                .verify();

        assertEquals(Arrays.asList(
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }


    @Test
    void testInnerUnitRollbackDoesNotAffectOuterCommit() {
        outer.onPrepareCommit(u -> Mono.when(inner.start()).then(inner.rollback(new MockException())));

        outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectComplete()
                .verify();


        assertEquals(Arrays.asList(new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void testRollbackOfMiddleUnitOfWorkRollsBackInner() {
        outer.onPrepareCommit(u ->
                Mono.when(middle.start())
                        .then(inner.start())
                        .then(inner.commit())
                        .then(middle.rollback())
        );

        outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectComplete()
                .verify();

        assertTrue(middle.isRolledBack(), "The middle UnitOfWork hasn't been correctly marked as rolled back");
        assertTrue(inner.isRolledBack(), "The inner UnitOfWork hasn't been correctly marked as rolled back");
        assertFalse(outer.isRolledBack(), "The out UnitOfWork has been incorrectly marked as rolled back");

        assertEquals(Arrays.asList(new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.COMMIT),
                // important that the inner has been given a rollback signal
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(middle, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(middle, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void testInnerUnitCommitFailureDoesNotAffectOuterCommit() {
        outer.onPrepareCommit(u ->
                Mono.when(inner.start())
                        .then(Mono.fromRunnable(()->{
                            inner.onCommit(uow-> Mono.error(new MockException()));
                            inner.onCommit(uow -> Mono.fromRunnable(() ->
                                    phaseTransitions.add(
                                            new PhaseTransition(uow, ReactiveUnitOfWork.Phase.COMMIT, "x")
                                    )));
                        }))
                        .then(inner.commit())
                        .onErrorResume(t->Mono.empty())
        );

        outer.start()
                .then(outer.commit())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .verifyComplete();

        assertEquals(Arrays.asList(new PhaseTransition(outer, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.COMMIT, "x"),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, ReactiveUnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, ReactiveUnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);

    }

    private static class PhaseTransition {

        private final ReactiveUnitOfWork.Phase phase;
        private final ReactiveUnitOfWork<?> unitOfWork;
        private final String id;

        public PhaseTransition(ReactiveUnitOfWork<?> unitOfWork, ReactiveUnitOfWork.Phase phase) {
            this(unitOfWork, phase, "");
        }

        public PhaseTransition(ReactiveUnitOfWork<?> unitOfWork, ReactiveUnitOfWork.Phase phase, String id) {
            this.unitOfWork = unitOfWork;
            this.phase = phase;
            this.id = id.length() > 0 ? " " + id : id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PhaseTransition that = (PhaseTransition) o;
            return Objects.equals(phase, that.phase) &&
                    Objects.equals(unitOfWork, that.unitOfWork) &&
                    Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, unitOfWork, id);
        }

        @Override
        public String toString() {
            return unitOfWork + " " + phase + id;
        }
    }

    private class MockException extends RuntimeException{};
}
