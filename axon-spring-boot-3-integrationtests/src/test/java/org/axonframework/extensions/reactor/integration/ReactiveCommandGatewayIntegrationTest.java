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

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.extensions.reactor.commandhandling.CommandBusStub;
import org.axonframework.extensions.reactor.commandhandling.gateway.ReactorCommandGateway;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class ReactiveCommandGatewayIntegrationTest {

    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
    }

    @Test
    void reactorCommandGatewayIsAvailable() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues()
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues()
                .run(context -> {
                    ReactorCommandGateway gateway = context.getBean(ReactorCommandGateway.class);
                    assertNotNull(gateway);
                    CommandBusStub commandBus = context.getBean(CommandBusStub.class);
                    assertNotNull(commandBus);
                    testResultFiltering(gateway, commandBus);
                });
    }

    void testResultFiltering(ReactorCommandGateway gateway, CommandBusStub commandBus) {
        registerResultsFilter(gateway, result -> result.getMetaData().containsKey("K"));
        // int 1 -> flux of results is filtered

        Mono<CommandResultMessage<?>> results = gateway.send("");
        StepVerifier.create(results).verifyComplete();
        // verify -> command has been sent
        assertEquals(1, commandBus.numberOfSentCommands());
    }

    private void registerResultsFilter(ReactorCommandGateway gateway,
                                       Predicate<CommandResultMessage<?>> predicate) {
        gateway.registerResultHandlerInterceptor((command, flux) -> flux.filter(predicate));
    }


    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

        @Bean
        CommandBus commandBus() {
            return new CommandBusStub();
        }
    }
}
