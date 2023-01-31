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

package org.axonframework.extensions.reactor.integration;

import org.axonframework.config.Configuration;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.extensions.reactor.eventhandling.gateway.ReactorEventGateway;
import org.axonframework.messaging.Message;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ReactiveEventGatewayIntegrationTest {

    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
    }

    @Test
    void reactorEventGatewayIsAvailable() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues()
                .run(context -> {
                    ReactorEventGateway gateway = context.getBean(ReactorEventGateway.class);
                    assertNotNull(gateway);
                    EventBus eventBus = context.getBean(EventBus.class);
                    assertNotNull(eventBus);
                    testPublish(gateway, eventBus);
                });
    }

    void testPublish(ReactorEventGateway gateway, EventBus eventBus) throws InterruptedException {
        Flux<Object> result = gateway.publish("event").map(Message::getPayload);

        StepVerifier.create(result).expectNext("event").verifyComplete();

        verify(eventBus).publish(any(EventMessage.class));
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

        @Bean
        EventBus eventBus(Configuration configuration) {
            return spy(SimpleEventBus.builder()
                                     .messageMonitor(configuration.messageMonitor(EventStore.class, "eventStore"))
                                     .spanFactory(configuration.spanFactory())
                                     .build());
        }
    }
}
