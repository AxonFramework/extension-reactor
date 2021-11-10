package org.axonframework.extensions.reactor.queryhandling.gateway;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptor;
import org.axonframework.extensions.reactor.messaging.ReactorResultHandlerInterceptor;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.DefaultSubscriptionQueryResult;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SubscriptionQueryBackpressure;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.messaging.GenericMessage.asMessage;

/**
 * Implementation of the {@link ReactorQueryGateway} that uses Project Reactor to achieve reactiveness.
 *
 * @author Milan Savic
 * @author Stefan Dragisic
 * @since 4.4.2
 */
public class DefaultReactorQueryGateway implements ReactorQueryGateway {

    private final List<ReactorMessageDispatchInterceptor<QueryMessage<?, ?>>> dispatchInterceptors;
    private final List<ReactorResultHandlerInterceptor<QueryMessage<?, ?>, ResultMessage<?>>> resultInterceptors;

    private final QueryBus queryBus;

    /**
     * Creates an instance of {@link DefaultReactorQueryGateway} based on the fields contained in the {@link
     * Builder}.
     * <p>
     * Will assert that the {@link QueryBus} is not {@code null} and throws an {@link AxonConfigurationException} if
     * it is.
     * </p>
     *
     * @param builder the {@link Builder} used to instantiated a {@link DefaultReactorQueryGateway} instance
     */
    protected DefaultReactorQueryGateway(Builder builder) {
        builder.validate();
        this.queryBus = builder.queryBus;
        this.dispatchInterceptors = new CopyOnWriteArrayList<>(builder.dispatchInterceptors);
        this.resultInterceptors = new CopyOnWriteArrayList<>(builder.resultInterceptors);
    }

    /**
     * Instantiate a Builder to be able to create a {@link DefaultReactorQueryGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@code resultHandlerInterceptors} are defaulted to an empty list.
     * The {@link QueryBus} is a <b>hard requirement</b> and as such should be provided.
     * </p>
     *
     * @return a Builder to be able to create a {@link DefaultReactorQueryGateway}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Registration registerDispatchInterceptor(
            ReactorMessageDispatchInterceptor<QueryMessage<?, ?>> interceptor) {
        dispatchInterceptors.add(interceptor);
        return () -> dispatchInterceptors.remove(interceptor);
    }

    @Override
    public Registration registerResultHandlerInterceptor(
            ReactorResultHandlerInterceptor<QueryMessage<?, ?>, ResultMessage<?>> interceptor) {
        resultInterceptors.add(interceptor);
        return () -> resultInterceptors.remove(interceptor);
    }

    /**
     * This is a utility method to intercept and transform inner elements emitted on {@code streamingQuery} result flux.
     * Using {@code registerResultHandlerInterceptor} will intercept {@code ResultMessage} which payload is reference to the flux,
     * and transforming inner elements from flux is hard to achieve with a lot of boilerplate code.
     * This method encapsulates this logic.
     *
     * @param interceptor The reactive interceptor to register (will be applied only to inner elements for streaming query result flux)
     * @return a Registration, which may be used to unregister the interceptor
     */
    public Registration registerStreamingQueryResultHandlerInterceptor(
            BiFunction<QueryMessage<?, ?>, ? super Flux<Object>, ? extends Publisher<Object>> interceptor) {
        return registerResultHandlerInterceptor((q, res) -> res.map(resultMessage -> {
            if (resultMessage.getPayload() instanceof Flux) {
                Flux<Object> payload = (Flux<Object>) resultMessage.getPayload();
                payload = payload.transform(f -> interceptor.apply(q, f));
                return GenericQueryResponseMessage.asResponseMessage(payload)
                        .andMetaData(resultMessage.getMetaData());
            } else {
                return resultMessage;
            }
        }));
    }

    @Override
    public <R, Q> Mono<R> query(String queryName, Q query, ResponseType<R> responseType) {
        return createQueryMessage(queryName, query, responseType)
                .transform(this::processDispatchInterceptors)
                .flatMap(this::dispatchQuery)
                .flatMapMany(this::processResultsInterceptors)
                .<R>transform(this::getPayload)
                .next();
    }

    public <R, Q> Mono<QueryMessage<?, ?>> createQueryMessage(String queryName, Q query, ResponseType<R> responseType) {
        return Mono.fromCallable(() -> new GenericQueryMessage<>(asMessage(query), queryName, responseType))
                .transformDeferredContextual((queryMono, contextView) ->
                        queryMono.map(q -> q.andMetaData(metaDataFromContext(contextView))));
    }

    public <R, Q> Mono<QueryMessage<?, ?>> createStreamableQueryMessage(String queryName, Q query, Class<R> responseType) {
        return Mono.fromCallable(() -> new GenericQueryMessage<>(asMessage(query), queryName, ResponseTypes.fluxOf(responseType)))
                .transformDeferredContextual((queryMono, contextView) ->
                        queryMono.map(q -> q.andMetaData(metaDataFromContext(contextView))));
    }

    private MetaData metaDataFromContext(ContextView contextView) {
        return contextView.getOrDefault(MetaData.class, MetaData.emptyInstance());
    }

    @Override
    public <R, Q> Flux<R> streamingQuery(String queryName, Q query, Class<R> responseType) {
        return createStreamableQueryMessage(queryName, query, responseType)
                .transform(this::processDispatchInterceptors)
                .flatMap(this::dispatchQuery)
                .flatMapMany(this::processResultsInterceptors)
                .<Flux<R>>transform(this::getPayload)
                .concatMap(Function.identity(), 0);
    }

    @Override
    public <R, Q> Flux<R> scatterGather(String queryName, Q query, ResponseType<R> responseType, Duration timeout) {
        return Mono.<QueryMessage<?, ?>>fromCallable(() -> new GenericQueryMessage<>(asMessage(query),
                        queryName,
                        responseType))
                .transform(this::processDispatchInterceptors)
                .flatMap(q -> dispatchScatterGatherQuery(q, timeout.toMillis(), TimeUnit.MILLISECONDS))
                .flatMapMany(this::processResultsInterceptors)
                .transform(this::getPayload);
    }

    @Override
    public <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(String queryName, Q query,
                                                                           ResponseType<I> initialResponseType,
                                                                           ResponseType<U> updateResponseType,
                                                                           SubscriptionQueryBackpressure backpressure,
                                                                           int updateBufferSize) {

        //noinspection unchecked
        return Mono.<QueryMessage<?, ?>>fromCallable(() -> new GenericSubscriptionQueryMessage<>(query,
                initialResponseType,
                updateResponseType))
                .transform(this::processDispatchInterceptors)
                .map(interceptedQuery -> (SubscriptionQueryMessage<Q, U, I>) interceptedQuery)
                .flatMap(isq -> dispatchSubscriptionQuery(isq, backpressure, updateBufferSize))
                .flatMap(processSubscriptionQueryResult());
    }


    private Mono<Tuple2<QueryMessage<?, ?>, Flux<ResultMessage<?>>>> dispatchQuery(QueryMessage<?, ?> queryMessage) {
        Flux<ResultMessage<?>> results = Flux
                .defer(() -> Mono.fromFuture(queryBus.query(queryMessage)));

        return Mono.<QueryMessage<?, ?>>just(queryMessage)
                .zipWith(Mono.just(results));
    }

    private Mono<Tuple2<QueryMessage<?, ?>, Flux<ResultMessage<?>>>> dispatchScatterGatherQuery(
            QueryMessage<?, ?> queryMessage, long timeout, TimeUnit timeUnit) {
        Flux<ResultMessage<?>> results = Flux
                .defer(() -> Flux.fromStream(queryBus.scatterGather(queryMessage,
                        timeout,
                        timeUnit)));

        return Mono.<QueryMessage<?, ?>>just(queryMessage)
                .zipWith(Mono.just(results));
    }

    private <Q, I, U> Mono<Tuple2<QueryMessage<Q, I>, Mono<SubscriptionQueryResult<QueryResponseMessage<I>, SubscriptionQueryUpdateMessage<U>>>>> dispatchSubscriptionQuery(
            SubscriptionQueryMessage<Q, I, U> queryMessage, SubscriptionQueryBackpressure backpressure,
            int updateBufferSize) {
        Mono<SubscriptionQueryResult<QueryResponseMessage<I>, SubscriptionQueryUpdateMessage<U>>> result = Mono
                .fromCallable(() -> queryBus.subscriptionQuery(queryMessage, backpressure, updateBufferSize));
        return Mono.<QueryMessage<Q, I>>just(queryMessage)
                .zipWith(Mono.just(result));
    }

    private Mono<QueryMessage<?, ?>> processDispatchInterceptors(Mono<QueryMessage<?, ?>> queryMessageMono) {
        return Flux.fromIterable(dispatchInterceptors)
                .reduce(queryMessageMono, (queryMessage, interceptor) -> interceptor.intercept(queryMessage))
                .flatMap(Mono::from);
    }

    private Flux<ResultMessage<?>> processResultsInterceptors(
            Tuple2<QueryMessage<?, ?>, Flux<ResultMessage<?>>> queryWithResponses) {
        QueryMessage<?, ?> queryMessage = queryWithResponses.getT1();
        Flux<ResultMessage<?>> queryResultMessage = queryWithResponses.getT2()
                .flatMapSequential(this::mapExceptionalResult);

        return Flux.fromIterable(resultInterceptors)
                .reduce(queryResultMessage,
                        (result, interceptor) -> interceptor.intercept(queryMessage, result))
                .flatMapMany(Function.identity());
    }

    private <Q, I, U> Function<Tuple2<QueryMessage<Q, U>,
            Mono<SubscriptionQueryResult<QueryResponseMessage<U>, SubscriptionQueryUpdateMessage<I>>>>,
            Mono<SubscriptionQueryResult<I, U>>> processSubscriptionQueryResult() {

        return messageWithResult -> messageWithResult.getT2().map(subscriptionResult -> {
            Mono<I> interceptedInitialResult = Mono.<QueryMessage<?, ?>>just(messageWithResult.getT1())
                    .zipWith(Mono.just(Flux.<ResultMessage<?>>from(subscriptionResult.initialResult())))
                    .flatMapMany(this::processResultsInterceptors)
                    .<I>transform(this::getPayload)
                    .next();

            Flux<U> interceptedUpdates = Mono.<QueryMessage<?, ?>>just(messageWithResult.getT1())
                    .zipWith(Mono.just(subscriptionResult.updates().<ResultMessage<?>>map(it -> it)))
                    .flatMapMany(this::processResultsInterceptors)
                    .transform(this::getPayload);

            return new DefaultSubscriptionQueryResult<>(interceptedInitialResult,
                    interceptedUpdates,
                    subscriptionResult);
        });
    }

    @SuppressWarnings("unchecked")
    private <R> Flux<R> getPayload(Flux<ResultMessage<?>> resultMessageFlux) {
        return resultMessageFlux
                .filter(r -> Objects.nonNull(r.getPayload()))
                .map(it -> (R) it.getPayload());
    }

    private Flux<? extends ResultMessage<?>> mapExceptionalResult(ResultMessage<?> result) {
        return result.isExceptional() ? Flux.error(result.exceptionResult()) : Flux.just(result);
    }

    /**
     * Builder class to instantiate {@link DefaultReactorQueryGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@link QueryBus} is a <b>hard requirement</b> and as such should be provided.
     * </p>
     */
    public static class Builder {

        private QueryBus queryBus;
        private List<ReactorMessageDispatchInterceptor<QueryMessage<?, ?>>> dispatchInterceptors = new CopyOnWriteArrayList<>();
        private List<ReactorResultHandlerInterceptor<QueryMessage<?, ?>, ResultMessage<?>>> resultInterceptors = new CopyOnWriteArrayList<>();

        /**
         * Sets the {@link QueryBus} used to dispatch queries.
         *
         * @param queryBus a {@link QueryBus} used to dispatch queries
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder queryBus(QueryBus queryBus) {
            assertNonNull(queryBus, "QueryBus may not be null");
            this.queryBus = queryBus;
            return this;
        }

        /**
         * Sets {@link ReactorMessageDispatchInterceptor}s for {@link QueryMessage}s. Are invoked
         * when a query is being dispatched.
         *
         * @param dispatchInterceptors which are invoked when a query is being dispatched
         * @return the current Builder instance, for fluent interfacing
         */
        @SafeVarargs
        public final Builder dispatchInterceptors(
                ReactorMessageDispatchInterceptor<QueryMessage<?, ?>>... dispatchInterceptors) {
            return dispatchInterceptors(asList(dispatchInterceptors));
        }

        /**
         * Sets the {@link List} of {@link ReactorMessageDispatchInterceptor}s for {@link QueryMessage}s. Are invoked
         * when a query is being dispatched.
         *
         * @param dispatchInterceptors which are invoked when a query is being dispatched
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder dispatchInterceptors(
                List<ReactorMessageDispatchInterceptor<QueryMessage<?, ?>>> dispatchInterceptors) {
            this.dispatchInterceptors = dispatchInterceptors != null && dispatchInterceptors.isEmpty()
                    ? new CopyOnWriteArrayList<>(dispatchInterceptors)
                    : new CopyOnWriteArrayList<>();
            return this;
        }

        /**
         * Sets {@link ReactorResultHandlerInterceptor}s for {@link ResultMessage}s.
         * Are invoked when a result has been received.
         *
         * @param resultInterceptors which are invoked when a result has been received
         * @return the current Builder instance, for fluent interfacing
         */
        @SafeVarargs
        public final Builder resultInterceptors(
                ReactorResultHandlerInterceptor<QueryMessage<?, ?>, ResultMessage<?>>... resultInterceptors) {
            return resultInterceptors(asList(resultInterceptors));
        }

        /**
         * Sets the {@link List} of {@link ReactorResultHandlerInterceptor}s for {@link ResultMessage}s.
         * Are invoked when a result has been received.
         *
         * @param resultInterceptors which are invoked when a result has been received
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder resultInterceptors(
                List<ReactorResultHandlerInterceptor<QueryMessage<?, ?>, ResultMessage<?>>> resultInterceptors) {
            this.resultInterceptors = resultInterceptors != null && !resultInterceptors.isEmpty()
                    ? new CopyOnWriteArrayList<>(resultInterceptors)
                    : new CopyOnWriteArrayList<>();
            return this;
        }


        /**
         * Validate whether the fields contained in this Builder as set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() {
            assertNonNull(queryBus, "The QueryBus is a hard requirement and should be provided");
        }

        /**
         * Initializes a {@link DefaultReactorQueryGateway} as specified through this Builder.
         *
         * @return a {@link DefaultReactorQueryGateway} as specified through this Builder
         */
        public DefaultReactorQueryGateway build() {
            return new DefaultReactorQueryGateway(this);
        }
    }
}
