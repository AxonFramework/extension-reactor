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

import org.axonframework.extensions.reactor.queryhandling.gateway.ReactorQueryGateway;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryMessage;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ReactiveQueryGatewayIntegrationTest {

    private ApplicationContextRunner testApplicationContext;
    private MessageHandler<QueryMessage<?, Object>> queryMessageHandler1;
    private MessageHandler<QueryMessage<?, Object>> queryMessageHandler2;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
        queryMessageHandler1 = spy(new MessageHandler<QueryMessage<?, Object>>() {
            @Override
            public Object handle(QueryMessage<?, Object> message) {
                return "handled";
            }
        });
        queryMessageHandler2 = spy(new MessageHandler<QueryMessage<?, Object>>() {
            @Override
            public Object handle(QueryMessage<?, Object> message) {
                return "handled";
            }
        });
    }

    @Test
    void reactorQueryGatewayIsAvailable() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues()
                .run(context -> {
                    QueryBus queryBus = context.getBean(QueryBus.class);
                    assertNotNull(queryBus);
                    registerHandlers(queryBus);
                    ReactorQueryGateway gateway = context.getBean(ReactorQueryGateway.class);
                    assertNotNull(gateway);
                    testQuery(gateway);
                });
    }

    private void registerHandlers(QueryBus queryBus) {
        queryBus.subscribe(String.class.getName(), String.class, queryMessageHandler1);
        queryBus.subscribe(String.class.getName(), String.class, queryMessageHandler2);
    }

    private void testQuery(ReactorQueryGateway gateway) throws Exception {

        Mono<String> result = gateway.query("criteria", String.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled")
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
        verifyNoMoreInteractions(queryMessageHandler2);
    }


    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}
