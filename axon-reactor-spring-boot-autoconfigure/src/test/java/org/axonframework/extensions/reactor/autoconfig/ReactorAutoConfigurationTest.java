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

package org.axonframework.extensions.reactor.autoconfig;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.extensions.reactor.commandhandling.gateway.DefaultReactorCommandGateway;
import org.axonframework.extensions.reactor.commandhandling.gateway.ReactorCommandGateway;
import org.axonframework.extensions.reactor.eventhandling.gateway.DefaultReactorEventGateway;
import org.axonframework.extensions.reactor.eventhandling.gateway.ReactorEventGateway;
import org.axonframework.extensions.reactor.queryhandling.gateway.DefaultReactorQueryGateway;
import org.axonframework.extensions.reactor.queryhandling.gateway.ReactorQueryGateway;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests Axon Framework Reactor extension auto configuration.
 *
 * @author Milan Savic
 */
@SpringBootTest
@ContextConfiguration
@EnableAutoConfiguration(exclude = {
        JmxAutoConfiguration.class,
        WebClientAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        DataSourceAutoConfiguration.class
})
class ReactorAutoConfigurationTest {

    @Autowired
    private ReactorCommandGateway reactorCommandGateway;

    @Autowired
    private ReactorQueryGateway reactorQueryGateway;

    @Autowired
    private ReactorEventGateway reactorEventGateway;

    @Test
    void testContextInitialization() {
        assertNotNull(reactorCommandGateway);
        assertNotNull(reactorQueryGateway);
        assertNotNull(reactorEventGateway);

        assertTrue(reactorCommandGateway instanceof DefaultReactorCommandGateway);
        assertTrue(reactorQueryGateway instanceof DefaultReactorQueryGateway);
        assertTrue(reactorEventGateway instanceof DefaultReactorEventGateway);
    }

    @Configuration
    public static class Context {

        @Bean
        public CommandBus commandBus() {
            return SimpleCommandBus.builder().build();
        }

        @Bean
        public QueryBus queryBus() {
            return SimpleQueryBus.builder().build();
        }

        @Bean
        public EventBus eventBus() {
            return SimpleEventBus.builder().build();
        }
    }
}
