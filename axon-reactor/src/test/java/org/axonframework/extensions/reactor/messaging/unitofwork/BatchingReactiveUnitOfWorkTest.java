package org.axonframework.extensions.reactor.messaging.unitofwork;

import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.unitofwork.ExecutionResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.axonframework.extensions.reactor.messaging.unitofwork.ReactiveUnitOfWork.Phase.*;
import static org.axonframework.messaging.GenericResultMessage.asResultMessage;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Stefan Dragisic
 */
class BatchingReactiveUnitOfWorkTest {

    private List<PhaseTransition> transitions;
    private BatchingReactiveUnitOfWork<?> subject;

    private static Message<?> toMessage(Object payload) {
        return new GenericMessage<>(payload);
    }

    public static Object resultFor(Message<?> message) {
        return "Result for: " + message.getPayload();
    }

    @BeforeEach
    void setUp() {
        transitions = new ArrayList<>();
    }

    @Test
    void testExecuteTask() throws Exception {
        List<Message<?>> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        subject = new BatchingReactiveUnitOfWork(messages);

        subject.executeWithResult(Mono.fromCallable(() -> {
            registerListeners(subject);
            return resultFor(subject.getMessage());
        }))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        validatePhaseTransitions(Arrays.asList(PREPARE_COMMIT, COMMIT, AFTER_COMMIT, CLEANUP), messages);
        Map<Message<?>, ExecutionResult> expectedResults = new HashMap<>();
        messages.forEach(m -> expectedResults.put(m, new ExecutionResult(asResultMessage(resultFor(m)))));
        assertExecutionResults(expectedResults, subject.getExecutionResults());
    }

    //todo backpressure

    @Test
    void testRollback() {
        List<Message<?>> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        subject = new BatchingReactiveUnitOfWork(messages);
        MockException e = new MockException();

        subject.executeWithResult(Mono.fromCallable(() -> {
            registerListeners(subject);
            if (subject.getMessage().getPayload().equals(1)) {
                throw e;
            }
            return resultFor(subject.getMessage());
        }))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        validatePhaseTransitions(Arrays.asList(ROLLBACK, CLEANUP), messages.subList(0, 2));
        Map<Message<?>, ExecutionResult> expectedResult = new HashMap<>();
        messages.forEach(m -> expectedResult.put(m, new ExecutionResult(asResultMessage(e))));
        assertExecutionResults(expectedResult, subject.getExecutionResults());
    }

    @Test
    void testSuppressedExceptionOnRollback() {
        List<Message<?>> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        AtomicInteger cleanupCounter = new AtomicInteger();

        subject = new BatchingReactiveUnitOfWork(messages);

        MockException taskException = new MockException("task exception");
        MockException commitException = new MockException("commit exception");
        MockException cleanupException = new MockException("cleanup exception");

        subject.onCleanupRun(u -> cleanupCounter.incrementAndGet());
        subject.onCleanup(u -> Mono.error(cleanupException));
        subject.onCleanupRun(u -> cleanupCounter.incrementAndGet());

        subject.executeWithResult(Mono.fromCallable(() -> {
            registerListeners(subject);
            if (subject.getMessage().getPayload().equals(2)) {
                subject.onPrepareCommit(u -> Mono.error(commitException));
                throw taskException;
            }
            return resultFor(subject.getMessage());
        }), e -> false)
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        validatePhaseTransitions(Arrays.asList(PREPARE_COMMIT, ROLLBACK, CLEANUP), messages);
        Map<Message<?>, ExecutionResult> expectedResult = new HashMap<>();
        expectedResult.put(messages.get(0), new ExecutionResult(asResultMessage(commitException)));
        expectedResult.put(messages.get(1), new ExecutionResult(asResultMessage(commitException)));
        expectedResult.put(messages.get(2), new ExecutionResult(asResultMessage(taskException)));
        assertExecutionResults(expectedResult, subject.getExecutionResults());
        assertSame(commitException, taskException.getSuppressed()[0]);
      //  assertEquals(2, cleanupCounter.get());
    }

    private ReactiveUnitOfWork<?> registerListeners(ReactiveUnitOfWork<?> unitOfWork) {
        unitOfWork.onPrepareCommitRun(u -> transitions.add(new PhaseTransition(u.getMessage(), ReactiveUnitOfWork.Phase.PREPARE_COMMIT)));
        unitOfWork.onCommitRun(u -> transitions.add(new PhaseTransition(u.getMessage(), ReactiveUnitOfWork.Phase.COMMIT)));
        unitOfWork.afterCommitRun(u ->transitions.add(new PhaseTransition(u.getMessage(), ReactiveUnitOfWork.Phase.AFTER_COMMIT)));
        unitOfWork.onRollbackRun(u -> transitions.add(new PhaseTransition(u.getMessage(), ReactiveUnitOfWork.Phase.ROLLBACK)));
        unitOfWork.onCleanupRun(u ->transitions.add(new PhaseTransition(u.getMessage(), ReactiveUnitOfWork.Phase.CLEANUP)));

        return unitOfWork;
    }

    private void validatePhaseTransitions(List<ReactiveUnitOfWork.Phase> phases, List<Message<?>> messages) {
        Iterator<PhaseTransition> iterator = transitions.iterator();
        for (ReactiveUnitOfWork.Phase phase : phases) {
            Iterator<Message<?>> messageIterator = phase.isReverseCallbackOrder()
                    ? new LinkedList<>(messages).descendingIterator() : messages.iterator();
            messageIterator.forEachRemaining(message -> {
                PhaseTransition expected = new PhaseTransition(message, phase);
                assertTrue(iterator.hasNext());
                PhaseTransition actual = iterator.next();
                assertEquals(expected, actual);
            });
        }
    }

    private void assertExecutionResults(Map<Message<?>, ExecutionResult> expected,
                                        Map<Message<?>, ExecutionResult> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        List<ResultMessage<?>> expectedMessages = expected.values()
                .stream()
                .map(ExecutionResult::getResult)
                .collect(Collectors.toList());

        List<ResultMessage<?>> actualMessages = actual.values()
                .stream()
                .map(ExecutionResult::getResult)
                .collect(Collectors.toList());
        List<?> expectedPayloads = expectedMessages.stream()
                .filter(crm -> !crm.isExceptional())
                .map(Message::getPayload)
                .collect(Collectors.toList());
        List<?> actualPayloads = actualMessages.stream()
                .filter(crm -> !crm.isExceptional())
                .map(Message::getPayload)
                .collect(Collectors.toList());
        List<Throwable> expectedExceptions = expectedMessages.stream()
                .filter(ResultMessage::isExceptional)
                .map(ResultMessage::exceptionResult)
                .collect(Collectors.toList());
        List<Throwable> actualExceptions = actualMessages.stream()
                .filter(ResultMessage::isExceptional)
                .map(ResultMessage::exceptionResult)
                .collect(Collectors.toList());
        List<MetaData> expectedMetaData = expectedMessages.stream()
                .map(Message::getMetaData)
                .collect(Collectors.toList());
        List<MetaData> actualMetaData = actualMessages.stream()
                .map(Message::getMetaData)
                .collect(Collectors.toList());
        assertEquals(expectedPayloads.size(), actualPayloads.size());
        assertTrue(expectedPayloads.containsAll(actualPayloads));
        assertEquals(expectedExceptions.size(), actualExceptions.size());
        assertTrue(expectedExceptions.containsAll(actualExceptions));
        assertTrue(expectedMetaData.containsAll(actualMetaData));
    }


    private static class PhaseTransition {

        private final ReactiveUnitOfWork.Phase phase;
        private final Message<?> message;

        public PhaseTransition(Message<?> message, ReactiveUnitOfWork.Phase phase) {
            this.message = message;
            this.phase = phase;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PhaseTransition that = (PhaseTransition) o;
            return phase == that.phase &&
                    Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, message);
        }

        @Override
        public String toString() {
            return phase + " -> " + message.getPayload();
        }
    }

    private class MockException extends RuntimeException {
        public MockException(String message) {
            super(message);
        }

        public MockException() {
            super();
        }
    }

}