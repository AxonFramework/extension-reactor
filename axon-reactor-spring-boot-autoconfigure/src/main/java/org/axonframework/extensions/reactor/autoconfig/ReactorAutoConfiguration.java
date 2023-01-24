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
import org.axonframework.eventhandling.EventBus;
import org.axonframework.extensions.reactor.commandhandling.gateway.DefaultReactorCommandGateway;
import org.axonframework.extensions.reactor.commandhandling.gateway.ReactorCommandGateway;
import org.axonframework.extensions.reactor.eventhandling.gateway.DefaultReactorEventGateway;
import org.axonframework.extensions.reactor.eventhandling.gateway.ReactorEventGateway;
import org.axonframework.extensions.reactor.queryhandling.gateway.DefaultReactorQueryGateway;
import org.axonframework.extensions.reactor.queryhandling.gateway.ReactorQueryGateway;
import org.axonframework.queryhandling.QueryBus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot Auto Configuration for Axon Framework Reactor extension.
 *
 * @author Milan Savic
 * @since 4.4.2
 */
@ConditionalOnClass(name = "reactor.core.publisher.Mono")
public class ReactorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ReactorCommandGateway reactiveCommandGateway(CommandBus commandBus) {
        return DefaultReactorCommandGateway.builder()
                                           .commandBus(commandBus)
                                           .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactorQueryGateway reactiveQueryGateway(QueryBus queryBus) {
        return DefaultReactorQueryGateway.builder()
                                         .queryBus(queryBus)
                                         .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactorEventGateway reactorEventGateway(EventBus eventBus) {
        return DefaultReactorEventGateway.builder()
                                         .eventBus(eventBus)
                                         .build();
    }
}
