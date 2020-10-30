package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.ConnectionFactory;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import org.axonframework.serialization.Serializer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * @author vtiwar27
 * @date 2020-10-27
 */
public class BlockingR2dbcEventStoreEngine implements EventStorageEngine {

    private final ReactiveEventStoreEngine reactiveEventStoreEngine;

    public static Builder builder() {
        return new BlockingR2dbcEventStoreEngine.Builder();
    }

    private BlockingR2dbcEventStoreEngine(Builder builder) {
        this.reactiveEventStoreEngine = new R2dbcEventStoreEngine(builder.connectionFactory,
                builder.schema, builder.dataType,
                builder.serializer);
    }

    @Override
    public void appendEvents(List<? extends EventMessage<?>> events) {
        reactiveEventStoreEngine.appendEvents(events).block();

    }

    @Override
    public void storeSnapshot(DomainEventMessage<?> snapshot) {
        reactiveEventStoreEngine.storeSnapshot(snapshot).block();
    }

    @Override
    public Stream<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken, boolean mayBlock) {
        return reactiveEventStoreEngine.readEvents(trackingToken).toStream();
    }

    @Override
    public DomainEventStream readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        return DomainEventStream.of(reactiveEventStoreEngine.readEvents(aggregateIdentifier, firstSequenceNumber).toStream());
    }

    @Override
    public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        return reactiveEventStoreEngine.lastSequenceNumberFor(aggregateIdentifier).block();
    }

    @Override
    public TrackingToken createTailToken() {
        return reactiveEventStoreEngine.createTailToken().block();
    }

    @Override
    public TrackingToken createHeadToken() {
        return reactiveEventStoreEngine.createHeadToken().block();
    }

    @Override
    public TrackingToken createTokenAt(Instant dateTime) {
        return reactiveEventStoreEngine.createTokenAt(dateTime).block();
    }


    public static class Builder {
        private Class<?> dataType = byte[].class;
        private EventSchema schema = new EventSchema();

        private ConnectionFactory connectionFactory;
        private Serializer serializer;

        public Builder connectionFactory(ConnectionFactory connectionFactory) {
            assertNonNull(connectionFactory, "connectionFactory may not be null");
            this.connectionFactory = connectionFactory;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        public Builder dataType(Class<?> dataType) {
            assertNonNull(dataType, "dataType may not be null");
            this.dataType = dataType;
            return this;
        }

        public Builder schema(EventSchema schema) {
            assertNonNull(schema, "EventSchema may not be null");
            this.schema = schema;
            return this;
        }

        public BlockingR2dbcEventStoreEngine build() {
            return new BlockingR2dbcEventStoreEngine(this);
        }

    }
}
