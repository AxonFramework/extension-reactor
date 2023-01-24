/*
 * Copyright (c) 2010-2023. Axon Framework
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

package org.axonframework.extensions.reactor.eventhandling.gateway;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.queryhandling.QueryMessage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static reactor.util.context.Context.of;

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
        Flux<Object> result = gateway.publish("event")
                                     .map(Message::getPayload);
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .expectNext("event")
                    .verifyComplete();

        verify(eventBus).publish(any(EventMessage.class));
    }

    @Test
    void testEventSetMetaDataViaContext() {
        Context context = of(MetaData.class, MetaData.with("k","v"));

        Flux<String> result = gateway.publish("event")
                                     .map(Message::getPayload)
                                     .cast(String.class)
                                     .contextWrite(c -> context);

        StepVerifier.create(result)
                    .expectNext("event")
                    .expectAccessibleContext()
                    .containsOnly(context)
                    .then()
                    .verifyComplete();


        ArgumentCaptor<GenericEventMessage> eventMessageCaptor =  ArgumentCaptor.forClass(GenericEventMessage.class);


        verify(eventBus).publish(eventMessageCaptor.capture());
        GenericEventMessage eventMessage = eventMessageCaptor.getValue();

        assertTrue(eventMessage.getMetaData().containsKey("k"));
        assertTrue(eventMessage.getMetaData().containsValue("v"));
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
        MetaData metaData = MetaData.with("key", "value");
        gateway.registerDispatchInterceptor(message -> message
                .map(event -> new GenericEventMessage<>("intercepted" + event.getPayload(), metaData)));

        Flux<EventMessage<?>> result = gateway.publish("event");
        verifyNoMoreInteractions(eventBus);

        StepVerifier.create(result)
                    .expectNextMatches(em -> "interceptedevent".equals(em.getPayload()) && metaData
                            .equals(em.getMetaData()))
                    .verifyComplete();
        verify(eventBus).publish(any(EventMessage.class));
    }

    @Test
    void testPublishOrder() {
        EventMessage<Object> event1 = GenericEventMessage.asEventMessage("event1");
        EventMessage<Object> event2 = GenericEventMessage.asEventMessage("event2");

        Flux<Object> result = gateway.publish(event1, event2)
                                     .map(Message::getPayload);
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

        Flux<Object> result = gateway.publishAll(events)
                                     .map(Message::getPayload);
        verifyNoMoreInteractions(eventBus);

        List<Throwable> exceptions = new ArrayList<>(3);
        StepVerifier.create(result.onErrorContinue((t, o) -> exceptions.add(t)))
                    .expectNext("event1", "event2")
                    .verifyComplete();

        assertEquals(Arrays.asList(exception1, exception2, exception3), exceptions);
        verify(eventBus, times(5)).publish(any(EventMessage.class));
    }
}
