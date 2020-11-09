package org.axonframework.extensions.reactor.eventstore.mappers;

import org.axonframework.eventhandling.GenericTrackedDomainEventMessage;
import org.axonframework.eventhandling.GenericTrackedEventMessage;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;

import java.util.stream.Stream;

/**
 * @author vtiwar27
 * @date 2020-11-03
 */
public class TrackedEventMessageMapper {

    private final Serializer serializer;
    private final EventUpcasterChain eventUpcasterChain;

    public TrackedEventMessageMapper(Serializer serializer,
                                     EventUpcasterChain eventUpcasterChain) {
        this.serializer = serializer;
        this.eventUpcasterChain = eventUpcasterChain;
    }


    public TrackedEventMessage<?> map(TrackedEventData row) {
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
                return new GenericTrackedDomainEventMessage<>(ir.getTrackingToken().get(),
                        ir.getAggregateType().orElse(null),
                        ir.getAggregateIdentifier().get(),
                        ir.getSequenceNumber().get(), serializedMessage,
                        ir::getTimestamp);
            } else {
                return new GenericTrackedEventMessage<>(ir.getTrackingToken().get(), serializedMessage,
                        ir::getTimestamp);
            }
        }).findFirst().get();
    }
}
