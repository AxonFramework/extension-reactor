package org.axonframework.extensions.reactor.poc.uow;

import org.axonframework.messaging.MetaData;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ReactiveCurrentUnitOfWork
 * @author Stefan Dragisic
 */
@SuppressWarnings({"rawtypes","unchecked"})
class ReactiveCurrentUnitOfWorkTest {

    @Test
    void expectContextPreserved() {
        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        ReactiveCurrentUnitOfWork.currentContext()
                .map(LinkedList::getFirst)
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(rouw)
                .verifyComplete();
    }


    @Test
    void isStarted() {
        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        ReactiveCurrentUnitOfWork.isStarted()
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void ifStarted() {
        AtomicBoolean started = new AtomicBoolean(false);

        Mono<String> doIfStarted = Mono.fromRunnable(()-> started.getAndSet(true));

        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        ReactiveCurrentUnitOfWork.ifStarted(root-> doIfStarted)
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        assertTrue(started.get());
    }

    @Test
    void map() {
        MetaData metaData = mock(MetaData.class);
        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        when(rouw.getCorrelationData()).thenReturn(metaData);

        ReactiveCurrentUnitOfWork.map(ReactiveUnitOfWork::getCorrelationData)
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(metaData)
                .verifyComplete();
    }

    @Test
    void commit() {
        AtomicBoolean committed = new AtomicBoolean(false);

        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        when(rouw.commit()).thenReturn(Mono.fromRunnable(()-> committed.getAndSet(true)));

        ReactiveCurrentUnitOfWork.commit()
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectComplete()
                .verify();

        assertTrue(committed.get());
    }

    @Test
    void clear() {
        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        ReactiveCurrentUnitOfWork.isStarted()
                .doOnNext(isStarted-> { if(!isStarted) throw new RuntimeException("not started");})
                .then(ReactiveCurrentUnitOfWork.clear(rouw))
                .then(ReactiveCurrentUnitOfWork.isStarted())
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void correlationData() {
        MetaData metaData = mock(MetaData.class);
        ReactiveUnitOfWork rouw = mock(ReactiveUnitOfWork.class);
        when(rouw.getCorrelationData()).thenReturn(metaData);

        ReactiveCurrentUnitOfWork.correlationData()
                .subscriberContext(ReactiveCurrentUnitOfWork.set(rouw))
                .as(StepVerifier::create)
                .expectNext(metaData)
                .verifyComplete();
    }
}