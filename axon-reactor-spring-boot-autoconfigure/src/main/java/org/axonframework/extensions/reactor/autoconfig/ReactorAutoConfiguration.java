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
 * @since 4.4
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
