package org.axonframework.extensions.reactor.eventhandling.gateway;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultReactorEventGateway}.
 *
 * @author Milan Savic
 */
@ExtendWith(MockitoExtension.class)
class DefaultReactorEventGatewayTest {

    private EventBus eventBus;
    private ReactorEventGateway gateway;

    @BeforeEach
    void setUp() {
        eventBus = mock(EventBus.class);
        gateway = DefaultReactorEventGateway.builder()
                                            .eventBus(eventBus)
                                            .build();
    }

    @Test
    void testPublish() {
        Flux<Object> result = gateway.publish("event");
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .expectNext("event")
                    .verifyComplete();

        verify(eventBus).publish(any(EventMessage.class));
    }

    @Test
    void testPublishWithError() {
        RuntimeException exception = new RuntimeException("oops");
        doThrow(exception).when(eventBus)
                          .publish(anyList());

        Flux<?> result = gateway.publish("event");
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .verifyError(RuntimeException.class);
        verify(eventBus).publish(any(EventMessage.class));
    }

    @Test
    void testDispatchInterceptor() {
        gateway.registerDispatchInterceptor(message -> message
                .map(event -> GenericEventMessage.asEventMessage("intercepted" + event.getPayload())));

        Flux<Object> result = gateway.publish("event");
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .expectNext("interceptedevent")
                    .verifyComplete();
        verify(eventBus).publish(any(EventMessage.class));
    }

    @Test
    void testPublishOrder() {
        EventMessage<Object> event1 = GenericEventMessage.asEventMessage("event1");
        EventMessage<Object> event2 = GenericEventMessage.asEventMessage("event2");

        Flux<Object> result = gateway.publish(event1, event2);
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .expectNext("event1", "event2")
                    .verifyComplete();
        verify(eventBus).publish(event1);
        verify(eventBus).publish(event2);
    }

    @Test
    void testPublishAll() {
        Flux<Object> events = Flux.fromIterable(Arrays.asList("event1", 4, "event2", 5, true));

        RuntimeException exception1 = new RuntimeException();
        RuntimeException exception2 = new RuntimeException();
        RuntimeException exception3 = new RuntimeException();
        doNothing()
                .doThrow(exception1)
                .doNothing()
                .doThrow(exception2)
                .doThrow(exception3)
                .when(eventBus)
                .publish(any(EventMessage.class));

        Flux<Object> result = gateway.publishAll(events);
        verifyNoMoreInteractions(eventBus);

        List<Throwable> exceptions = new ArrayList<>(3);
        StepVerifier.create(result.onErrorContinue((t, o) -> exceptions.add(t)))
                    .expectNext("event1", "event2")
                    .verifyComplete();

        assertEquals(Arrays.asList(exception1, exception2, exception3), exceptions);
        verify(eventBus, times(5)).publish(any(EventMessage.class));
    }
}
