package org.axonframework.extensions.reactor.poc.uow;

import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.ReactiveTransactionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * TODO DOC
 * @author Stefan Dragisic
 */
class DefaultUnitOfWorkTest {

    GenericEventMessage<String> genericEventMessage = new GenericEventMessage<>("Event 1");
    GenericCommandMessage<String> genericCommandMessage = new GenericCommandMessage<>("Command 1");

    ReactiveTransactionManager reactiveTransactionManagerMock = mock(ReactiveTransactionManager.class);
    ReactiveTransaction reactiveTransactionMock = mock(ReactiveTransaction.class);

    ReactiveSpringTransactionManager reactiveSpringTransactionManager =
            new ReactiveSpringTransactionManager(reactiveTransactionManagerMock);

    @BeforeEach
    void setup() {
        Hooks.onOperatorDebug();
        Hooks.enableContextLossTracking();

        when(reactiveTransactionMock.isNewTransaction()).thenReturn(true);

        when(reactiveTransactionManagerMock.commit(any())).thenReturn(Mono.empty());
        when(reactiveTransactionManagerMock.rollback(any())).thenReturn(Mono.empty());

        when(reactiveTransactionMock.isCompleted()).thenReturn(false);
        when(reactiveTransactionManagerMock.getReactiveTransaction(any())).thenReturn(Mono.just(reactiveTransactionMock));
    }

    @Test
    void ouwIsActiveAfterStart() {
        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                .as(StepVerifier::create)
                .assertNext(ReactiveUnitOfWork::isActive)
                .verifyComplete();

    }

    @Test
    void uowHasContextAfterStart() {
        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                .then(ReactiveCurrentUnitOfWork.isStarted())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void twoUowInTransaction() {
        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                .then(DefaultReactiveUnitOfWork.startAndGet(genericEventMessage))
                .then(ReactiveCurrentUnitOfWork.currentContext().map(LinkedList::size))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNext(2)
                .verifyComplete();
    }

    @Test
    void commit() {
        List<Mono<DefaultReactiveUnitOfWork<GenericEventMessage<String>>>> uows =
                Arrays.asList(
                        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage),
                        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage),
                        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                );

        Flux.fromIterable(uows)
                .flatMap(Function.identity())
                .flatMap(ReactiveUnitOfWork::commit)
                .then(ReactiveCurrentUnitOfWork.isEmpty())
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void rollbackCompleted() {
        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                .flatMap(uow -> uow.rollback().then(Mono.just(uow)))
                .map(AbstractReactiveUnitOfWork::phase)
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectNext(ReactiveUnitOfWork.Phase.CLOSED)
                .verifyComplete();
    }

    @Test
    void rollbackFails() {
        DefaultReactiveUnitOfWork.startAndGet(genericEventMessage)
                .doOnNext(ouw -> ouw.setPhase(ReactiveUnitOfWork.Phase.CLOSED))
                .flatMap(ReactiveUnitOfWork::rollback)
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .expectErrorMatches(t -> t.getLocalizedMessage().startsWith("The UnitOfWork is in an incompatible phase"))
                .verify();
    }

    @Test
    void executeWithResult() {
        Mono<String> monoTask = Mono.fromCallable(() -> "executed");

        DefaultReactiveUnitOfWork.startAndGet(genericCommandMessage)
                .flatMap(uow -> uow.executeWithResult(monoTask))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .assertNext(r -> assertEquals(r.getPayload(), "executed"))
                .verifyComplete();
    }

    @Test
    void executeWithError() {
        Throwable runtimeException = new RuntimeException("error");
        Mono<String> monoTask = Mono.error(runtimeException);

        DefaultReactiveUnitOfWork.startAndGet(genericCommandMessage)
                .flatMap(uow -> uow.executeWithResult(monoTask))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .assertNext(r -> assertEquals(runtimeException, r.exceptionResult()))
                .verifyComplete();
    }

    @Test
    void attachTransactionSimple() {
        Mono.just(new DefaultReactiveUnitOfWork<>(genericCommandMessage)) // create uow
                .flatMap(uow -> uow.attachTransaction(reactiveSpringTransactionManager).then(Mono.just(uow))) // attach transaction
                .flatMap(uow -> uow.start().then(uow.commit())) // commit uow
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .verifyComplete();


        verify(reactiveTransactionManagerMock, times(1)).commit(any());
        verify(reactiveTransactionManagerMock, times(0)).rollback(any());
    }

    @Test
    void attachTransactionFull() {
        String payload = "executed";
        Mono<String> monoTask = Mono.fromCallable(() -> payload)
                .delayElement(Duration.ofSeconds(1));

        DefaultReactiveUnitOfWork.startAndGet(genericCommandMessage)
                .flatMap(uow -> uow.attachTransaction(reactiveSpringTransactionManager).then(Mono.just(uow)))
                .flatMap(uow -> uow.executeWithResult(monoTask))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .assertNext(r -> assertEquals(r.getPayload(), payload))
                .verifyComplete();

        verify(reactiveTransactionManagerMock, times(1)).commit(any());
        verify(reactiveTransactionManagerMock, times(0)).rollback(any());
    }

    @Test
    void attachTransactionWithRollback() {
        Throwable runtimeException = new RuntimeException("error");
        Mono<String> monoTask = Mono.<String>error(runtimeException)
                .delaySubscription(Duration.ofSeconds(1));

        DefaultReactiveUnitOfWork.startAndGet(genericCommandMessage)
                .flatMap(uow -> uow.attachTransaction(reactiveSpringTransactionManager).then(Mono.just(uow)))
                .flatMap(uow -> uow.executeWithResult(monoTask))
                .as(UnitOfWorkOperators::executionContext)
                .as(StepVerifier::create)
                .assertNext(r -> assertEquals(runtimeException, r.exceptionResult()))
                .verifyComplete();

        verify(reactiveTransactionManagerMock, times(0)).commit(any());
        verify(reactiveTransactionManagerMock, times(1)).rollback(any());
    }

}