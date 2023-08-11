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

package org.axonframework.extensions.reactor.queryhandling.gateway;

import org.axonframework.common.Registration;
import org.axonframework.messaging.GenericResultMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryUpdateEmitter;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.junit.jupiter.api.*;
import org.mockito.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.axonframework.common.ReflectionUtils.methodOf;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static reactor.util.context.Context.of;

/**
 * Tests for {@link DefaultReactorQueryGateway}.
 *
 * @author Milan Savic
 * @author Stefan Dragisic
 */
class DefaultReactorQueryGatewayTest {

    private QueryBus queryBus;
    private QueryUpdateEmitter queryUpdateEmitter;

    private MessageHandler<QueryMessage<?, Object>> queryMessageHandler1;
    private MessageHandler<QueryMessage<?, Object>> queryMessageHandler2;
    private MessageHandler<QueryMessage<?, Object>> queryMessageHandler3;
    private MessageHandler<QueryMessage<?, Flux<Long>>> queryMessageHandler4;

    private DefaultReactorQueryGateway testSubject;

    @SuppressWarnings({"resource", "Convert2Lambda"})
    @BeforeEach
    void setUp() throws NoSuchMethodException {
        Hooks.enableContextLossTracking();
        Hooks.onOperatorDebug();

        queryBus = spy(SimpleQueryBus.builder().build());

        queryUpdateEmitter = queryBus.queryUpdateEmitter();
        AtomicInteger count = new AtomicInteger();
        queryMessageHandler1 = spy(new MessageHandler<QueryMessage<?, Object>>() {

            @Override
            public Object handle(QueryMessage<?, Object> message) {
                if ("backpressure".equals(message.getPayload())) {
                    return count.incrementAndGet();
                }
                return "handled";
            }
        });
        queryMessageHandler2 = spy(new MessageHandler<QueryMessage<?, Object>>() {
            @Override
            public Object handle(QueryMessage<?, Object> message) {
                if ("backpressure".equals(message.getPayload())) {
                    return count.incrementAndGet();
                }
                return "handled";
            }
        });

        queryMessageHandler3 = spy(new MessageHandler<QueryMessage<?, Object>>() {
            @Override
            public Object handle(QueryMessage<?, Object> message) {
                throw new RuntimeException();
            }
        });

        queryMessageHandler4 = spy(new MessageHandler<QueryMessage<?, Flux<Long>>>() {
            @Override
            public Object handle(QueryMessage<?, Flux<Long>> message) {
                //noinspection ReactiveStreamsUnusedPublisher
                return Flux.range(1, 10)
                           .map(Long::valueOf)
                           .delayElements(Duration.ofMillis(50));
            }
        });

        queryBus.subscribe(String.class.getName(), String.class, queryMessageHandler1);
        queryBus.subscribe(String.class.getName(), String.class, queryMessageHandler2);
        queryBus.subscribe(Integer.class.getName(), Integer.class, queryMessageHandler3);
        queryBus.subscribe(Long.class.getName(), Long.class, queryMessageHandler4);

        queryBus.subscribe(Boolean.class.getName(),
                           String.class,
                           message -> "" + message.getMetaData().getOrDefault("key1", "")
                                   + message.getMetaData().getOrDefault("key2", ""));

        queryBus.subscribe(Long.class.getName(), String.class, message -> null);

        queryBus.subscribe(Double.class.getName(),
                           methodOf(this.getClass(), "stringListQueryHandler").getGenericReturnType(),
                           message -> Arrays.asList("value1", "value2", "value3"));

        testSubject = DefaultReactorQueryGateway.builder()
                                                .queryBus(queryBus)
                                                .build();
    }

    @SuppressWarnings("unused") // Used by 'testSubscriptionQueryMany()' to generate query handler response type
    public List<String> stringListQueryHandler() {
        return new ArrayList<>();
    }

    @Test
    void directQuery() throws Exception {
        Mono<String> result = testSubject.query("criteria", String.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled")
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void directQueryWithContext() throws Exception {
        Context context = of(MetaData.class, MetaData.with("k", "v"));

        Mono<String> result = testSubject.query("criteria", String.class)
                                         .contextWrite(c -> context);

        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled")
                    .expectAccessibleContext()
                    .containsAllOf(context)
                    .then()
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());

        //noinspection unchecked
        ArgumentCaptor<QueryMessage<?, ?>> queryMessageCaptor = ArgumentCaptor.forClass(QueryMessage.class);

        verify(queryBus).query(queryMessageCaptor.capture());
        QueryMessage<?, ?> queryMessage = queryMessageCaptor.getValue();

        assertTrue(queryMessage.getMetaData().containsKey("k"));
        assertTrue(queryMessage.getMetaData().containsValue("v"));
    }

    @Test
    void querySetMetaDataViaContext() throws Exception {
        Context context = Context.of("k1", "v1");

        Mono<String> result = testSubject.query("criteria", String.class)
                                         .contextWrite(c -> context);

        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled")
                    .expectAccessibleContext()
                    .containsAllOf(context)
                    .then()
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void multipleDirectQueries() throws Exception {
        Flux<QueryMessage<?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericQueryMessage<>("query1", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(4, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>("query2", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(5, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>(true, ResponseTypes.instanceOf(String.class))));

        Flux<Object> result = testSubject.query(queries);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        List<Throwable> exceptions = new ArrayList<>(2);
        StepVerifier.create(result.onErrorContinue((t, o) -> exceptions.add(t)))
                    .expectNext("handled", "handled", "")
                    .verifyComplete();

        assertEquals(2, exceptions.size());
        assertTrue(exceptions.get(0) instanceof RuntimeException);
        assertTrue(exceptions.get(1) instanceof RuntimeException);
        verify(queryMessageHandler1, times(2)).handle(any());
    }

    @Test
    void multipleDirectQueriesOrdered() throws Exception {
        int numberOfQueries = 10_000;
        Flux<QueryMessage<?, ?>> queries = Flux
                .fromStream(IntStream.range(0, numberOfQueries)
                                     .mapToObj(i -> new GenericQueryMessage<>("backpressure",
                                                                              ResponseTypes.instanceOf(String.class))));
        Flux<Object> result = testSubject.query(queries);
        StepVerifier.create(result)
                    .expectNext(IntStream.range(1, numberOfQueries + 1)
                                         .boxed()
                                         .toArray(Integer[]::new))
                    .verifyComplete();
        verify(queryMessageHandler1, times(numberOfQueries)).handle(any());
    }

    @Test
    void directQueryReturningNull() {
        assertNull(testSubject.query(0L, String.class).block());
        StepVerifier.create(testSubject.query(0L, String.class))
                    .expectComplete()
                    .verify();
    }

    @Test
    void directQueryWithDispatchInterceptor() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key1", "value1")))
        );
        //noinspection resource
        Registration registration2 = testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key2", "value2")))
        );

        StepVerifier.create(testSubject.query(true, String.class))
                    .expectNext("value1value2")
                    .verifyComplete();

        registration2.cancel();

        StepVerifier.create(testSubject.query(true, String.class))
                    .expectNext("value1")
                    .verifyComplete();
    }

    @Test
    void directQueryWithBuilderDispatchInterceptor() {
        ReactorQueryGateway reactorQueryGateway =
                DefaultReactorQueryGateway.builder()
                                          .queryBus(queryBus)
                                          .dispatchInterceptors(queryMono -> queryMono.map(
                                                  query -> query.andMetaData(MetaData.with("key1", "value1")))
                                          )
                                          .build();

        StepVerifier.create(reactorQueryGateway.query(true, String.class))
                    .expectNext("value1")
                    .verifyComplete();
    }

    @Test
    void directQueryWithErrorDispatchInterceptor() {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor((q, r) -> r.onErrorMap(t -> new MockException()));

        StepVerifier.create(testSubject.query(5, Integer.class)) //throws Runtime exception by default
                    .verifyError(MockException.class);
    }

    @Test
    void directQueryWithDispatchInterceptorWithContext() {
        Context context = Context.of("security", true);

        //noinspection resource
        testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.filterWhen(v -> Mono.deferContextual(Mono::just)
                                                           .filter(ctx -> ctx.hasKey("security"))
                                                           .map(ctx -> ctx.get("security")))
                                      .map(query -> query.andMetaData(Collections.singletonMap("key1", "value1")))
        );

        StepVerifier.create(testSubject.query(true, String.class).contextWrite(c -> context))
                    .expectNext("value1")
                    .expectAccessibleContext()
                    .containsAllOf(context)
                    .then()
                    .verifyComplete();
    }

    @Test
    void directQueryWithResultInterceptorAlterResult() throws Exception {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor(
                (q, results) -> results.map(it -> new GenericQueryResponseMessage<>("handled-modified"))
        );

        Mono<String> result = testSubject.query("criteria", String.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled-modified")
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void directQueryWithResultInterceptorFilterResult() throws Exception {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor(
                (q, results) -> results.filter(it -> !it.getPayload().equals("handled"))
        );

        Mono<String> result = testSubject.query("criteria", String.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectComplete()
                    .verify();
        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void directQueryWithDispatchInterceptorThrowingAnException() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> {
            throw new RuntimeException();
        });
        StepVerifier.create(testSubject.query(true, String.class))
                    .verifyError(RuntimeException.class);
    }

    @Test
    void directQueryWithDispatchInterceptorReturningErrorMono() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> Mono.error(new RuntimeException()));
        StepVerifier.create(testSubject.query(true, String.class))
                    .verifyError(RuntimeException.class);
    }

    @Test
    void directQueryFails() {
        StepVerifier.create(testSubject.query(5, Integer.class))
                    .verifyError(RuntimeException.class);
    }

    @Test
    void directQueryFailsWithRetry() throws Exception {
        Mono<Integer> query = testSubject.query(5, Integer.class).retry(5);

        StepVerifier.create(query)
                    .verifyError(RuntimeException.class);

        verify(queryMessageHandler3, times(6)).handle(any());
    }


    @Test
    void scatterGather() throws Exception {
        Flux<String> result = testSubject.scatterGather("criteria",
                                                        ResponseTypes.instanceOf(String.class),
                                                        Duration.ofSeconds(1));
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        StepVerifier.create(result)
                    .expectNext("handled", "handled")
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
        verify(queryMessageHandler2).handle(any());
    }

    @Test
    void multipleScatterGather() throws Exception {
        Flux<QueryMessage<?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericQueryMessage<>("query1", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(4, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>("query2", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(5, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>(true, ResponseTypes.instanceOf(String.class))));

        Flux<Object> result = testSubject.scatterGather(queries, Duration.ofSeconds(1));
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        StepVerifier.create(result)
                    .expectNext("handled", "handled", "handled", "handled", "")
                    .verifyComplete();

        verify(queryMessageHandler1, times(2)).handle(any());
        verify(queryMessageHandler2, times(2)).handle(any());
    }

    @Test
    void multipleScatterGatherOrdering() throws Exception {
        int numberOfQueries = 10_000;
        Flux<QueryMessage<?, ?>> queries = Flux
                .fromStream(IntStream.range(0, numberOfQueries)
                                     .mapToObj(i -> new GenericQueryMessage<>("backpressure",
                                                                              ResponseTypes.instanceOf(String.class))));
        Flux<Object> result = testSubject.scatterGather(queries, Duration.ofSeconds(1));
        StepVerifier.create(result)
                    .expectNext(IntStream.range(1, 2 * numberOfQueries + 1)
                                         .boxed()
                                         .toArray(Integer[]::new))
                    .verifyComplete();
        verify(queryMessageHandler1, times(numberOfQueries)).handle(any());
        verify(queryMessageHandler2, times(numberOfQueries)).handle(any());
    }

    @Test
    void scatterGatherReturningNull() {
        assertNull(testSubject.scatterGather(0L, ResponseTypes.instanceOf(String.class), Duration.ofSeconds(1))
                              .blockFirst());
        StepVerifier.create(testSubject.scatterGather(0L,
                                                      ResponseTypes.instanceOf(String.class),
                                                      Duration.ofSeconds(1)))
                    .expectNext()
                    .verifyComplete();
    }

    @Test
    void scatterGatherWithDispatchInterceptor() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key1", "value1")))
        );
        //noinspection resource
        Registration registration2 = testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key2", "value2")))
        );

        StepVerifier.create(testSubject.scatterGather(
                            true, ResponseTypes.instanceOf(String.class), Duration.ofSeconds(1)
                    ))
                    .expectNext("value1value2")
                    .verifyComplete();

        registration2.cancel();

        StepVerifier.create(testSubject.query(true, String.class))
                    .expectNext("value1")
                    .verifyComplete();
    }

    @Test
    void scatterGatherWithResultIntercept() throws Exception {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor(
                (query, results) -> results.map(it -> new GenericQueryResponseMessage<>("handled-modified"))
        );

        Flux<QueryMessage<?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericQueryMessage<>("query1", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(4, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>("query2", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(5, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>(true, ResponseTypes.instanceOf(String.class))));

        Flux<Object> result = testSubject.scatterGather(queries, Duration.ofSeconds(1));
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        StepVerifier.create(result)
                    .expectNext("handled-modified",
                                "handled-modified",
                                "handled-modified",
                                "handled-modified",
                                "handled-modified")
                    .verifyComplete();

        verify(queryMessageHandler1, times(2)).handle(any());
        verify(queryMessageHandler2, times(2)).handle(any());
    }

    @Test
    void streamingQuery() throws Exception {
        Flux<Long> result = testSubject.streamingQuery(1L, Long.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        verifyNoMoreInteractions(queryMessageHandler3);
        StepVerifier.create(result)
                    .expectNext(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
                    .verifyComplete();
        verify(queryMessageHandler4).handle(any());
    }

    @Test
    void streamingQueryInterceptor() throws Exception {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor((q, res) -> res.take(4).map(resultMessage -> {
            Long value = (Long) resultMessage.getPayload();
            Long newValue = value * 2;
            return new GenericResultMessage<Object>(newValue, resultMessage.getMetaData());
        }));

        Flux<Long> result = testSubject.streamingQuery(1L, Long.class);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        verifyNoMoreInteractions(queryMessageHandler3);
        StepVerifier.create(result)
                    .expectNext(2L, 4L, 6L, 8L)
                    .verifyComplete();
        verify(queryMessageHandler4).handle(any());
    }

    @Test
    void scatterGatherWithResultInterceptorModifyResultBasedOnQuery() throws Exception {
        //noinspection resource
        testSubject.registerDispatchInterceptor(
                q -> q.map(it -> it.andMetaData(Collections.singletonMap("block", it.getPayload() instanceof Boolean)))
        );
        //noinspection resource
        testSubject.registerResultHandlerInterceptor(
                (q, results) -> results.filter(it -> !((boolean) q.getMetaData().get("block")))
        );

        Flux<QueryMessage<?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericQueryMessage<>("query1", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(4, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>("query2", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(5, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>(Boolean.TRUE, ResponseTypes.instanceOf(String.class))));

        Flux<Object> result = testSubject.scatterGather(queries, Duration.ofSeconds(1));
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        StepVerifier.create(result)
                    .expectNext("handled", "handled", "handled", "handled")
                    .verifyComplete();

        verify(queryMessageHandler1, times(2)).handle(any());
        verify(queryMessageHandler2, times(2)).handle(any());
    }

    @Test
    void scatterGatherWithResultInterceptReplacedWithError() throws Exception {
        //noinspection resource
        testSubject.registerResultHandlerInterceptor(
                (query, results) -> results.flatMap(r -> {
                    if (r.getPayload().equals("")) {
                        return Flux.error(new RuntimeException("no empty strings allowed"));
                    } else {
                        return Flux.just(r);
                    }
                })
        );

        Flux<QueryMessage<?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericQueryMessage<>("query1", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(4, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>("query2", ResponseTypes.instanceOf(String.class)),
                new GenericQueryMessage<>(5, ResponseTypes.instanceOf(Integer.class)),
                new GenericQueryMessage<>(true, ResponseTypes.instanceOf(String.class))));

        Flux<Object> result = testSubject.scatterGather(queries, Duration.ofSeconds(1));
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        StepVerifier.create(result)
                    .expectNext("handled", "handled", "handled", "handled")
                    .expectError(RuntimeException.class)
                    .verify();

        verify(queryMessageHandler1, times(2)).handle(any());
        verify(queryMessageHandler2, times(2)).handle(any());
    }

    @Test
    void scatterGatherWithDispatchInterceptorThrowingAnException() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> {
            throw new RuntimeException();
        });
        StepVerifier.create(testSubject.scatterGather(
                            true, ResponseTypes.instanceOf(String.class), Duration.ofSeconds(1))
                    )
                    .verifyError(RuntimeException.class);
    }

    @Test
    void scatterGatherWithDispatchInterceptorReturningErrorMono() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> Mono.error(new RuntimeException()));

        StepVerifier.create(testSubject.scatterGather(
                true, ResponseTypes.instanceOf(String.class), Duration.ofSeconds(1))
        ).verifyError(RuntimeException.class);
    }

    @Test
    void scatterGatherFails() {
        StepVerifier.create(testSubject.scatterGather(
                            6, ResponseTypes.instanceOf(Integer.class), Duration.ofSeconds(1))
                    )
                    .expectNextCount(0)
                    .verifyComplete();
    }

    @Test
    void scatterGatherFailsWithRetry() {
        doThrow(new RuntimeException(":("))
                .when(queryBus).scatterGather(any(), anyLong(), any());

        Flux<Integer> query =
                testSubject.scatterGather(6, ResponseTypes.instanceOf(Integer.class), Duration.ofSeconds(1)).retry(5);

        StepVerifier.create(query)
                    .verifyError();

        verify(queryBus, times(6)).scatterGather(any(), anyLong(), any());
    }


    @Test
    void subscriptionQuery() throws Exception {
        Mono<SubscriptionQueryResult<String, String>> monoResult =
                testSubject.subscriptionQuery("criteria", String.class, String.class);

        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        SubscriptionQueryResult<String, String> result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult())
                    .expectNext("handled")
                    .verifyComplete();
        StepVerifier.create(
                            result.updates()
                                  .doOnSubscribe(s -> {
                                      queryUpdateEmitter.emit(String.class, q -> true, "update");
                                      queryUpdateEmitter.complete(String.class, q -> true);
                                  })
                    )
                    .expectNext("update")
                    .verifyComplete();
        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void multipleSubscriptionQueries() throws Exception {
        Flux<SubscriptionQueryMessage<?, ?, ?>> queries = Flux.fromIterable(Arrays.asList(
                new GenericSubscriptionQueryMessage<>("query1",
                                                      ResponseTypes.instanceOf(String.class),
                                                      ResponseTypes.instanceOf(String.class)),
                new GenericSubscriptionQueryMessage<>(4,
                                                      ResponseTypes.instanceOf(Integer.class),
                                                      ResponseTypes.instanceOf(String.class))));

        Flux<SubscriptionQueryResult<?, ?>> result = testSubject.subscriptionQuery(queries);
        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);
        List<Mono<Object>> initialResults = new ArrayList<>(2);
        //noinspection unchecked
        result.subscribe(sqr -> initialResults.add((Mono<Object>) sqr.initialResult()));
        assertEquals(2, initialResults.size());
        StepVerifier.create(initialResults.get(0))
                    .expectNext("handled")
                    .verifyComplete();
        StepVerifier.create(initialResults.get(1))
                    .verifyError(RuntimeException.class);

        verify(queryMessageHandler1).handle(any());
    }

    @Test
    void multipleSubscriptionQueriesOrdering() throws Exception {
        int numberOfQueries = 10_000;
        ResponseType<String> responseType = ResponseTypes.instanceOf(String.class);

        Flux<SubscriptionQueryMessage<?, ?, ?>> queries = Flux.fromStream(
                IntStream.range(0, numberOfQueries)
                         .mapToObj(i -> new GenericSubscriptionQueryMessage<>(
                                 "backpressure", responseType, responseType)
                         )
        );

        List<Integer> expectedResults = IntStream.range(1, numberOfQueries + 1)
                                                 .boxed()
                                                 .collect(Collectors.toList());
        Flux<SubscriptionQueryResult<?, ?>> result = testSubject.subscriptionQuery(queries);
        List<Mono<Object>> initialResults = new ArrayList<>(numberOfQueries);
        //noinspection unchecked
        result.subscribe(sqr -> initialResults.add((Mono<Object>) sqr.initialResult()));
        assertEquals(numberOfQueries, initialResults.size());
        for (int i = 0; i < numberOfQueries; i++) {
            StepVerifier.create(initialResults.get(i))
                        .expectNext(expectedResults.get(i))
                        .verifyComplete();
        }

        verify(queryMessageHandler1, times(numberOfQueries)).handle(any());
    }

    @Test
    void subscriptionQueryReturningNull() {
        SubscriptionQueryResult<String, String> result =
                testSubject.subscriptionQuery(0L, String.class, String.class).block();

        assertNotNull(result);
        assertNull(result.initialResult().block());
        StepVerifier.create(result.initialResult())
                    .expectNext()
                    .verifyComplete();
        StepVerifier.create(
                            result.updates()
                                  .doOnSubscribe(s -> {
                                      queryUpdateEmitter.emit(Long.class, q -> true, (String) null);
                                      queryUpdateEmitter.complete(Long.class, q -> true);
                                  })
                    )
                    .expectNext()
                    .verifyComplete();
    }

    @Test
    void subscriptionQueryWithDispatchInterceptor() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key1", "value1")))
        );
        //noinspection resource
        Registration registration2 = testSubject.registerDispatchInterceptor(
                queryMono -> queryMono.map(query -> query.andMetaData(Collections.singletonMap("key2", "value2")))
        );

        Mono<SubscriptionQueryResult<String, String>> monoResult =
                testSubject.subscriptionQuery(true, String.class, String.class);
        SubscriptionQueryResult<String, String> result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult())
                    .expectNext("value1value2")
                    .verifyComplete();

        registration2.cancel();

        monoResult = testSubject.subscriptionQuery(true, String.class, String.class);
        result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult())
                    .expectNext("value1")
                    .verifyComplete();
    }

    @Test
    void subscriptionQueryWithDispatchInterceptorThrowingAnException() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> {
            throw new RuntimeException();
        });

        StepVerifier.create(testSubject.subscriptionQuery(true, String.class, String.class))
                    .verifyError(RuntimeException.class);
    }

    @Test
    void subscriptionQueryWithDispatchInterceptorReturningErrorMono() {
        //noinspection resource
        testSubject.registerDispatchInterceptor(queryMono -> Mono.error(new RuntimeException()));

        StepVerifier.create(testSubject.subscriptionQuery(true, String.class, String.class))
                    .verifyError(RuntimeException.class);
    }

    @Test
    void subscriptionQueryFails() {
        Mono<SubscriptionQueryResult<Integer, Integer>> monoResult =
                testSubject.subscriptionQuery(6, Integer.class, Integer.class);

        SubscriptionQueryResult<Integer, Integer> result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult())
                    .verifyError(RuntimeException.class);
    }

    @Test
    void subscriptionQueryFailsRetryInitialDispatchQuery() {
        //noinspection resource,deprecation
        doThrow(new RuntimeException(":("))
                .when(queryBus)
                .subscriptionQuery(any(), any(), anyInt());

        Mono<SubscriptionQueryResult<Integer, Integer>> monoResult =
                testSubject.subscriptionQuery(6, Integer.class, Integer.class).retry(5);

        StepVerifier.create(monoResult)
                    .verifyError(RuntimeException.class);

        //noinspection resource,deprecation
        verify(queryBus, times(6)).subscriptionQuery(any(), any(), anyInt());
    }

    @Test
    void subscriptionQueryFailsRetryInitialResult() throws Exception {
        Mono<SubscriptionQueryResult<Integer, Integer>> monoResult =
                testSubject.subscriptionQuery(6, Integer.class, Integer.class);

        SubscriptionQueryResult<Integer, Integer> result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult().retry(5))
                    .verifyError(RuntimeException.class);

        verify(queryMessageHandler3, times(6)).handle(any());
    }

    @Test
    void subscriptionQuerySingleInitialResultAndUpdates() {
        Flux<String> result = testSubject.subscriptionQuery("6", String.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.execute(() -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }
            queryUpdateEmitter.emit(String.class, q -> true, "1");
            queryUpdateEmitter.emit(String.class, q -> true, "2");
            queryUpdateEmitter.emit(String.class, q -> true, "3");
            queryUpdateEmitter.emit(String.class, q -> true, "4");
            queryUpdateEmitter.emit(String.class, q -> true, "5");
            queryUpdateEmitter.complete(String.class, q -> true);
        });

        StepVerifier.create(result.doOnNext(s -> countDownLatch.countDown()))
                    .expectNext("handled")
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    @Test
    void queryUpdates() {
        //noinspection resource,deprecation
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();

            queryUpdateEmitter.emit(String.class, q -> true, "1");
            queryUpdateEmitter.emit(String.class, q -> true, "2");
            queryUpdateEmitter.emit(String.class, q -> true, "3");
            queryUpdateEmitter.emit(String.class, q -> true, "4");
            queryUpdateEmitter.emit(String.class, q -> true, "5");
            queryUpdateEmitter.complete(String.class, q -> true);

            return result;
        }).when(queryBus)
          .subscriptionQuery(any(), any(), anyInt());

        StepVerifier.create(testSubject.queryUpdates("6", String.class))
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    @Test
    void subscriptionQueryMany() {
        //noinspection resource,deprecation
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();

            queryUpdateEmitter.emit(Double.class, q -> true, "update1");
            queryUpdateEmitter.emit(Double.class, q -> true, "update2");
            queryUpdateEmitter.emit(Double.class, q -> true, "update3");
            queryUpdateEmitter.emit(Double.class, q -> true, "update4");
            queryUpdateEmitter.emit(Double.class, q -> true, "update5");
            queryUpdateEmitter.complete(Double.class, p -> true);

            return result;
        }).when(queryBus)
          .subscriptionQuery(any(), any(), anyInt());

        StepVerifier.create(testSubject.subscriptionQueryMany(2.3, String.class))
                    .expectNext("value1", "value2", "value3", "update1", "update2", "update3", "update4", "update5")
                    .verifyComplete();
    }

    @Test
    void dispatchSubscriptionQueryWithMetaDataByProvidingMessageInstanceAsPayload() throws Exception {
        ResponseType<String> responseType = ResponseTypes.instanceOf(String.class);
        GenericSubscriptionQueryMessage<String, String, String> testQuery =
                new GenericSubscriptionQueryMessage<>("criteria", responseType, responseType)
                        .withMetaData(MetaData.with("key", "value"));

        Mono<SubscriptionQueryResult<String, String>> monoResult =
                testSubject.subscriptionQuery(testQuery, String.class, String.class);

        verifyNoMoreInteractions(queryMessageHandler1);
        verifyNoMoreInteractions(queryMessageHandler2);

        SubscriptionQueryResult<String, String> result = monoResult.block();
        assertNotNull(result);
        StepVerifier.create(result.initialResult())
                    .expectNext("handled")
                    .verifyComplete();

        verify(queryBus).subscriptionQuery(
                argThat(subscriptionQuery -> "value".equals(subscriptionQuery.getMetaData().get("key"))),
                any(),
                anyInt()
        );
    }

    private static class MockException extends RuntimeException {

    }
}
