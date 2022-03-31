package org.axonframework.extensions.reactor.queryhandling.gateway;

import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptorSupport;
import org.axonframework.extensions.reactor.messaging.ReactorResultHandlerInterceptorSupport;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryGateway;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryBackpressure;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.function.Function;

import static org.axonframework.queryhandling.QueryMessage.queryName;

/**
 * Variation of the {@link QueryGateway}, wrapping a {@link QueryBus} for a friendlier API. Provides support for reactive return types such as {@link Mono} and {@link Flux}
 * from Project Reactor.
 *
 * @author Milan Savic
 * @since 4.4.2
 */
public interface ReactorQueryGateway extends ReactorMessageDispatchInterceptorSupport<QueryMessage<?, ?>>,
        ReactorResultHandlerInterceptorSupport<QueryMessage<?, ?>, ResultMessage<?>> {

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response with the given {@code responseType}
     * from a single source. The query name will be derived from the provided {@code query}. Execution may be
     * asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>Do note that the {@code query} will not be dispatched until there is a subscription to the resulting {@link
     * Mono}</b></p>
     *
     * @param query        The {@code query} to be sent
     * @param responseType A {@link Class} describing the desired response type
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Mono} containing the query result as dictated by the given {@code responseType}
     */
    default <R, Q> Mono<R> query(Q query, Class<R> responseType) {
        return query(QueryMessage.queryName(query), query, responseType);
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response with the given {@code responseType}
     * from a single source. Execution may be asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>Do note that the {@code query} will not be dispatched until there is a subscription to the resulting {@link
     * Mono}</b></p>
     *
     * @param queryName    A {@link String} describing the query to be executed
     * @param query        The {@code query} to be sent
     * @param responseType The {@link ResponseType} used for this query
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Mono} containing the query result as dictated by the given {@code responseType}
     */
    default <R, Q> Mono<R> query(String queryName, Q query, Class<R> responseType) {
        return query(queryName, query, ResponseTypes.instanceOf(responseType));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response in the form of {@code responseType}
     * from a single source. The query name will be derived from the provided {@code query}. Execution may be
     * asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>Do note that the {@code query} will not be dispatched until there is a subscription to the resulting {@link
     * Mono}</b></p>
     *
     * @param query        The {@code query} to be sent
     * @param responseType The {@link ResponseType} used for this query
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Mono} containing the query result as dictated by the given {@code responseType}
     */
    default <R, Q> Mono<R> query(Q query, ResponseType<R> responseType) {
        return query(QueryMessage.queryName(query), query, responseType);
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response in the form of {@code responseType}
     * from a single source. Execution may be asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>Do note that the {@code query} will not be dispatched until there is a subscription to the resulting {@link
     * Mono}</b></p>
     *
     * @param queryName    A {@link String} describing the query to be executed
     * @param query        The {@code query} to be sent
     * @param responseType The {@link ResponseType} used for this query
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Mono} containing the query result as dictated by the given {@code responseType}
     */
    <R, Q> Mono<R> query(String queryName, Q query, ResponseType<R> responseType);

    /**
     * Use the given {@link Publisher} of {@link QueryMessage}s to send the incoming queries away. Queries will be sent sequentially. Once the
     * result of the Nth query arrives, the (N + 1)th query is dispatched.
     *
     * @param queries a {@link Publisher} stream of queries to be dispatched
     * @return a {@link Flux} of query results. The ordering of query results corresponds to the ordering of queries being
     * dispatched
     *
     * @see #query(String, Object, ResponseType)
     * @see Flux#concatMap(Function)
     */
    default Flux<Object> query(Publisher<QueryMessage<?, ?>> queries) {
        return Flux.from(queries)
                   .concatMap(q -> query(q.getQueryName(), q.getPayload(), q.getResponseType()));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response in the form of {@code responseType}
     * from several sources. The returned {@link Flux} is completed when a {@code timeout} occurs or when all possible
     * results are received. The query name will be derived from the provided {@code query}. Execution may be
     * asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <b>Note</b>: Any {@code null} results will be filtered out by the {@link ReactorQueryGateway}. If you require
     * the {@code null} to be returned, we suggest using {@code QueryBus} instead.
     *
     * @param query        The {@code query} to be sent
     * @param responseType The {@link ResponseType} used for this query
     * @param timeout      A timeout of {@code long} for the query
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Flux} containing the query results as dictated by the given {@code responseType}
     */
    default <R, Q> Flux<R> scatterGather(Q query, ResponseType<R> responseType, Duration timeout) {
        return scatterGather(QueryMessage.queryName(query), query, responseType, timeout);
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, expecting a response in the form of {@code responseType}
     * from several sources. The returned {@link Flux} is completed when a {@code timeout} occurs or when all results
     * are received. Execution may be asynchronous, depending on the {@code QueryBus} implementation.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <b>Note</b>: Any {@code null} results will be filtered out by the {@link ReactorQueryGateway}. If you require
     * the {@code null} to be returned, we suggest using {@code QueryBus} instead.
     *
     * @param queryName    A {@link String} describing the query to be executed
     * @param query        The {@code query} to be sent
     * @param responseType The {@link ResponseType} used for this query
     * @param timeout      A timeout of {@code long} for the query
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Flux} containing the query results as dictated by the given {@code responseType}
     */
    <R, Q> Flux<R> scatterGather(String queryName, Q query, ResponseType<R> responseType, Duration timeout);

    /**
     * Uses the given {@link Publisher} of {@link QueryMessage}s to send incoming queries in scatter gather manner. Queries will be sent
     * sequentially. Once the result of Nth query arrives, the (N + 1)th query is dispatched. All queries will be dispatched
     * using given {@code timeout} and {@code timeUnit}.
     *
     * @param queries  a {@link Publisher} stream of queries to be dispatched
     * @param timeout  A timeout of {@code long} for the query
     * @return a {@link Flux} of query results. The ordering of query results corresponds to the ordering of queries
     * being dispatched
     * @see Flux#concatMap(Function)
     */
    default Flux<Object> scatterGather(Publisher<QueryMessage<?, ?>> queries, Duration timeout) {
        return Flux.from(queries)
                   .concatMap(q -> scatterGather(q.getQueryName(),
                                                 q.getPayload(),
                                                 q.getResponseType(),
                                                 timeout));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, returning the initial result and a stream of incremental
     * updates, received at the moment the query is sent, until it is cancelled by the caller or closed by the emitting
     * side. Should be used when the response type of the initial result and incremental update match.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Flux}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query      The {@code query} to be sent
     * @param resultType The response type used for this query
     * @param <Q>        The type of the query
     * @param <R>        The type of the result (initial and updates)
     * @return Flux which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, R> Flux<R> subscriptionQuery(Q query, ResponseType<R> resultType) {
        return subscriptionQuery(query, resultType, resultType)
                .flatMapMany(result -> result.initialResult()
                                             .concatWith(result.updates())
                                             .doFinally(signal -> result.close()));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, returns initial result and keeps streaming incremental
     * updates, received at the moment the query is sent, until it is cancelled by the caller or closed by the emitting
     * side. Should be used when response type of initial result and incremental update match.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Flux}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query      The {@code query} to be sent
     * @param resultType The response type used for this query
     * @param <Q>        The type of the query
     * @param <R>        The type of the result (initial and updates)
     * @return Flux which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, R> Flux<R> subscriptionQuery(Q query, Class<R> resultType) {
        return subscriptionQuery(query, ResponseTypes.instanceOf(resultType));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, returns initial result and keeps streaming incremental
     * updates, received at the moment the query is sent, until it is cancelled by the caller or closed by the emitting
     * side. Should be used when initial result contains multiple instances of response type and needs to be flattened.
     * Response type of initial response and incremental updates needs to match.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Flux}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query      The {@code query} to be sent
     * @param resultType The response type used for this query
     * @param <Q>        The type of the query
     * @param <R>        The type of the result (initial and updates)
     * @return Flux which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, R> Flux<R> subscriptionQueryMany(Q query, Class<R> resultType) {
        return subscriptionQuery(query,
                                 ResponseTypes.multipleInstancesOf(resultType),
                                 ResponseTypes.instanceOf(resultType))
                .flatMapMany(result -> result.initialResult()
                                             .flatMapMany(Flux::fromIterable)
                                             .concatWith(result.updates())
                                             .doFinally(signal -> result.close()));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, streaming incremental updates, received at the moment
     * the query is sent, until it is cancelled by the caller or closed by the emitting side.
     * Should be used when the subscriber is interested only in updates.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Flux}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query      The {@code query} to be sent
     * @param resultType The response type used for this query
     * @param <Q>        The type of the query
     * @param <R>        The type of the result (updates)
     * @return Flux which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, R> Flux<R> queryUpdates(Q query, ResponseType<R> resultType) {
        return subscriptionQuery(query, ResponseTypes.instanceOf(Void.class), resultType)
                .flatMapMany(result -> result.updates()
                                             .doFinally(signal -> result.close()));
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus}, streaming incremental updates, received at the moment
     * the query is sent, until it is cancelled by the caller or closed by the emitting side.
     * Should be used when the subscriber is interested only in updates.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Flux}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query      The {@code query} to be sent
     * @param resultType The response type used for this query
     * @param <Q>        The type of the query
     * @param <R>        The type of the result (updates)
     * @return Flux which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, R> Flux<R> queryUpdates(Q query, Class<R> resultType) {
        return queryUpdates(query, ResponseTypes.instanceOf(resultType));
    }

    /**
     * Sends the given {@code query} over the {@link org.axonframework.queryhandling.QueryBus}, expecting a {@link Flux} response
     * of {@code responseType}. The {@code query} is send once the returned {@code Flux} is subscribed to.
     * The streaming query allows a client to stream larger result sets if required.
     *
     * @param query        The {@code query} to be send
     * @param responseType A {@link java.lang.Class} describing the desired response type
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Flux} streaming the results as dictated by the given {@code responseType}
     */
    default <R, Q> Flux<R> streamingQuery(Q query, Class<R> responseType) {
        return streamingQuery(queryName(query), query, responseType);
    }

    /**
     * Sends the given {@code query} with {@code queryName} over the {@link org.axonframework.queryhandling.QueryBus}, 
     * expecting a {@link Flux} response of {@code responseType}. The {@code query} is send once the
     *  returned {@code Flux} is subscribed to. The streaming query allows a client to stream larger result sets if required.
     *
     * @param queryName    A {@link java.lang.String} describing the query to be executed
     * @param query        The {@code query} to be send
     * @param responseType A {@link java.lang.Class} describing the desired response type
     * @param <R>          The response class contained in the given {@code responseType}
     * @param <Q>          The query class
     * @return A {@link Flux} streaming the results as dictated by the given {@code responseType}
     */
    <R, Q> Flux<R> streamingQuery(String queryName, Q query, Class<R> responseType);

    /**
     * Sends the given {@code query} over the {@link QueryBus} and returns result containing initial response and
     * incremental updates, received at the moment the query is sent, until it is cancelled by the caller or closed by
     * the emitting side.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query               The {@code query} to be sent
     * @param initialResponseType The initial response type used for this query
     * @param updateResponseType  The update response type used for this query
     * @param <Q>                 The type of the query
     * @param <I>                 The type of the initial response
     * @param <U>                 The type of the incremental update
     * @return registration which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(Q query, Class<I> initialResponseType,
                                                                            Class<U> updateResponseType) {
        return subscriptionQuery(QueryMessage.queryName(query),
                                 query,
                                 initialResponseType,
                                 updateResponseType);
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus} and returns result containing initial response and
     * incremental updates, received at the moment the query is sent, until it is cancelled by the caller or closed by
     * the emitting side.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param queryName           A {@link String} describing query to be executed
     * @param query               The {@code query} to be sent
     * @param initialResponseType The initial response type used for this query
     * @param updateResponseType  The update response type used for this query
     * @param <Q>                 The type of the query
     * @param <I>                 The type of the initial response
     * @param <U>                 The type of the incremental update
     * @return registration which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(String queryName, Q query,
                                                                            Class<I> initialResponseType,
                                                                            Class<U> updateResponseType) {
        return subscriptionQuery(queryName,
                                 query,
                                 ResponseTypes.instanceOf(initialResponseType),
                                 ResponseTypes.instanceOf(updateResponseType),
                                 SubscriptionQueryBackpressure.defaultBackpressure());
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus} and returns result containing initial response and
     * incremental updates, received at the moment the query is sent, until it is cancelled by the caller or closed by
     * the emitting side.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param query               The {@code query} to be sent
     * @param initialResponseType The initial response type used for this query
     * @param updateResponseType  The update response type used for this query
     * @param <Q>                 The type of the query
     * @param <I>                 The type of the initial response
     * @param <U>                 The type of the incremental update
     * @return registration which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(Q query,
                                                                            ResponseType<I> initialResponseType,
                                                                            ResponseType<U> updateResponseType) {
        return subscriptionQuery(QueryMessage.queryName(query),
                                 query,
                                 initialResponseType,
                                 updateResponseType,
                                 SubscriptionQueryBackpressure.defaultBackpressure());
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus} and returns result containing initial response and
     * incremental updates, received at the moment the query is sent, until it is cancelled by the caller or closed by
     * the emitting side.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param queryName           A {@link String} describing query to be executed
     * @param query               The {@code query} to be sent
     * @param initialResponseType The initial response type used for this query
     * @param updateResponseType  The update response type used for this query
     * @param backpressure        The backpressure mechanism to deal with producing of incremental updates
     * @param <Q>                 The type of the query
     * @param <I>                 The type of the initial response
     * @param <U>                 The type of the incremental update
     * @return registration which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    default <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(String queryName, Q query,
                                                                            ResponseType<I> initialResponseType,
                                                                            ResponseType<U> updateResponseType,
                                                                            SubscriptionQueryBackpressure backpressure) {
        return subscriptionQuery(queryName,
                                 query,
                                 initialResponseType,
                                 updateResponseType,
                                 backpressure,
                                 Queues.SMALL_BUFFER_SIZE);
    }

    /**
     * Sends the given {@code query} over the {@link QueryBus} and returns result containing initial response and
     * incremental updates, received at the moment the query is sent, until it is cancelled by the caller or closed by
     * the emitting side.
     * <p><b>{@code query} will not be dispatched until there is a subscription to the resulting {@link Mono}</b></p>
     * <p>
     * <b>Note</b>: Any {@code null} results, on the initial result or the updates, will be filtered out by the
     * {@link ReactorQueryGateway}. If you require the {@code null} to be returned for the initial and update results,
     * we suggest using the {@code QueryBus} instead.
     *
     * @param queryName           A {@link String} describing query to be executed
     * @param query               The {@code query} to be sent
     * @param initialResponseType The initial response type used for this query
     * @param updateResponseType  The update response type used for this query
     * @param backpressure        The backpressure mechanism to deal with producing of incremental updates
     * @param updateBufferSize    The size of buffer which accumulates updates before subscription to the {@code flux}
     *                            is made
     * @param <Q>                 The type of the query
     * @param <I>                 The type of the initial response
     * @param <U>                 The type of the incremental update
     * @return registration which can be used to cancel receiving updates
     *
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage)
     * @see QueryBus#subscriptionQuery(SubscriptionQueryMessage, SubscriptionQueryBackpressure, int)
     */
    <Q, I, U> Mono<SubscriptionQueryResult<I, U>> subscriptionQuery(String queryName, Q query,
                                                                    ResponseType<I> initialResponseType,
                                                                    ResponseType<U> updateResponseType,
                                                                    SubscriptionQueryBackpressure backpressure,
                                                                    int updateBufferSize);

    /**
     * Uses the given {@link Publisher} of {@link SubscriptionQueryMessage}s to send incoming queries away. Queries will
     * be sent sequentially. Once the result of Nth query arrives, the (N + 1)th query is dispatched.
     *
     * @param queries a {@link Publisher} stream of queries to be dispatched
     * @return a {@link Flux} of query results. The ordering of query results corresponds to the ordering of queries
     * being dispatched
     *
     * @see #subscriptionQuery(String, Object, Class, Class)
     * @see Flux#concatMap(Function)
     */
    default Flux<SubscriptionQueryResult<?, ?>> subscriptionQuery( // NOSONAR
                                                                   Publisher<SubscriptionQueryMessage<?, ?, ?>> queries) {
        return subscriptionQuery(queries, SubscriptionQueryBackpressure.defaultBackpressure());
    }

    /**
     * Uses the given {@link Publisher} of {@link SubscriptionQueryMessage}s to send incoming queries away. Queries will
     * be sent sequentially. Once the result of Nth query arrives, the (N + 1)th query is dispatched. All queries will
     * be dispatched using the given {@code backpressure}.
     *
     * @param queries      a {@link Publisher} stream of queries to be dispatched
     * @param backpressure the backpressure mechanism to deal with producing of incremental updates
     * @return a {@link Flux} of query results. The ordering of query results corresponds to the ordering of queries being
     * dispatched
     *
     * @see #subscriptionQuery(String, Object, Class, Class)
     * @see Flux#concatMap(Function)
     */
    default Flux<SubscriptionQueryResult<?, ?>> subscriptionQuery( // NOSONAR
                                                                   Publisher<SubscriptionQueryMessage<?, ?, ?>> queries,
                                                                   SubscriptionQueryBackpressure backpressure) {
        return subscriptionQuery(queries, backpressure, Queues.SMALL_BUFFER_SIZE);
    }

    /**
     * Uses the given {@link Publisher} of {@link SubscriptionQueryMessage}s to send incoming queries away. Queries will
     * be sent sequentially. Once the result of Nth query arrives, the (N + 1)th query is dispatched. All queries will
     * be dispatched using given {@code backpressure} and {@code updateBufferSize}.
     *
     * @param queries          a {@link Publisher} stream of queries to be dispatched
     * @param backpressure     the backpressure mechanism to deal with producing of incremental updates
     * @param updateBufferSize the size of buffer which accumulates updates before subscription to the {@code flux}
     *                         is made
     * @return a {@link Flux} of query results. The ordering of query results corresponds to the ordering of queries being
     * dispatched
     *
     * @see #subscriptionQuery(String, Object, Class, Class)
     * @see Flux#concatMap(Function)
     */
    default Flux<SubscriptionQueryResult<?, ?>> subscriptionQuery( // NOSONAR
                                                                   Publisher<SubscriptionQueryMessage<?, ?, ?>> queries,
                                                                   SubscriptionQueryBackpressure backpressure,
                                                                   int updateBufferSize) {
        return Flux.from(queries)
                   .concatMap(q -> subscriptionQuery(q.getQueryName(),
                                                     q.getPayload(),
                                                     q.getResponseType(),
                                                     q.getUpdateResponseType(),
                                                     backpressure,
                                                     updateBufferSize));
    }
}
