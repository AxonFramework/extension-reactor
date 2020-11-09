package org.axonframework.extensions.reactor.eventstore.impl;

import io.r2dbc.spi.ConnectionFactory;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.ReactiveEventStoreEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * @author vtiwar27
 * @date 2020-10-27
 */
public class BlockingR2dbcEventStoreEngine extends BatchingEventStorageEngine {

    private final ReactiveEventStoreEngine reactiveEventStoreEngine;

    public static Builder builder() {
        return new BlockingR2dbcEventStoreEngine.Builder();
    }

    private BlockingR2dbcEventStoreEngine(Builder builder) {
        super(builder);
        this.reactiveEventStoreEngine =
                R2dbcEventStoreEngine.builder().
                        connectionFactory(builder.connectionFactory)
                        .schema(builder.schema)
                        .eventSerializer(builder.serializer)
                        .dataType(builder.dataType)
                        .upcasterChain(builder.upcasterChain)
                        .extendedGapCheckEnabled(builder.extendedGapCheckEnabled)
                        .batchSize(builder.batchSize).build();
    }


    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        reactiveEventStoreEngine.appendEvents(events).block();
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        reactiveEventStoreEngine.storeSnapshot(snapshot).block();
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        return reactiveEventStoreEngine.readSnapshotData(aggregateIdentifier).toStream();
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

    public void createSchema() {
        reactiveEventStoreEngine.createSchema().block();
    }

    public void executeSql(String sql) {
        reactiveEventStoreEngine.executeSql(sql).block();
    }

    @Override
    public List<? extends TrackedEventData<?>> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        return this.reactiveEventStoreEngine.readEvents(lastToken, batchSize).toStream().collect(Collectors.toList());
    }

    @Override
    protected List<? extends DomainEventData<?>> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber, int batchSize) {
        return this.reactiveEventStoreEngine.readEvents(aggregateIdentifier, firstSequenceNumber, batchSize).toStream().collect(Collectors.toList());
    }


    public static class Builder extends BatchingEventStorageEngine.Builder {
        private Class<?> dataType = byte[].class;
        private EventSchema schema = new EventSchema();

        private ConnectionFactory connectionFactory;
        private Serializer serializer = XStreamSerializer.defaultSerializer();
        private int batchSize;
        private EventUpcaster upcasterChain;
        private boolean extendedGapCheckEnabled;

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

        public Builder extendedGapCheckEnabled(boolean extendedGapCheckEnabled) {
            assertNonNull(extendedGapCheckEnabled, "extendedGapCheckEnabled may not be null");
            this.extendedGapCheckEnabled = extendedGapCheckEnabled;
            return this;
        }

        public Builder schema(EventSchema schema) {
            assertNonNull(schema, "EventSchema may not be null");
            this.schema = schema;
            return this;
        }


        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            this.batchSize = batchSize;

            return this;
        }


        @Override
        public Builder upcasterChain(EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            this.upcasterChain = upcasterChain;
            return this;
        }

        @Override
        public Builder snapshotFilter(Predicate<? super DomainEventData<?>> snapshotFilter) {
            super.snapshotFilter(snapshotFilter);
            return this;
        }

        public BlockingR2dbcEventStoreEngine build() {
            return new BlockingR2dbcEventStoreEngine(this);
        }

    }
}
