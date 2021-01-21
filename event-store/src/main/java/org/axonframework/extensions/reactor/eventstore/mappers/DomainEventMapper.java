package org.axonframework.extensions.reactor.eventstore.mappers;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;

import java.util.stream.Stream;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class DomainEventMapper {

    private final Serializer serializer;
    private final EventUpcasterChain eventUpcasterChain;

    public DomainEventMapper(Serializer serializer, EventUpcasterChain eventUpcasterChain) {
        this.serializer = serializer;
        this.eventUpcasterChain = eventUpcasterChain;
    }


    public DomainEventMessage<?> map(DomainEventData row) {

        final InitialEventRepresentation initialEventRepresentation = new InitialEventRepresentation(row, serializer);
        final Stream<IntermediateEventRepresentation> upcast = eventUpcasterChain.
                upcast(Stream.of(initialEventRepresentation));
        return upcast.map(ir -> {
            SerializedMessage<?> serializedMessage = new SerializedMessage<>(ir.getMessageIdentifier(),
                    new LazyDeserializingObject<>(
                            ir::getData,
                            ir.getType(), serializer),
                    ir.getMetaData());
            if (ir.getAggregateIdentifier().isPresent()) {
                return new GenericDomainEventMessage<>(
                        ir.getAggregateType().orElse(null),
                        ir.getAggregateIdentifier().get(),
                        ir.getSequenceNumber().get(), serializedMessage,
                        ir::getTimestamp);
            } else {
                return new GenericDomainEventMessage<>(ir.getAggregateType().get(),
                        ir.getAggregateIdentifier().get(),
                        ir.getSequenceNumber().get(), serializedMessage,
                        ir::getTimestamp);
            }
        }).findFirst().get();
    }
}
