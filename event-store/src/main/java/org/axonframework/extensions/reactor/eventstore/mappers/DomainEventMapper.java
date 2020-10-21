package org.axonframework.extensions.reactor.eventstore.mappers;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.eventhandling.GenericDomainEventMessage;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class DomainEventMapper {


    public static DomainEventMessage<?> map(GenericDomainEventEntry genericDomainEventEntry) {
        //TODO upcasting to be added
        return new GenericDomainEventMessage<>(genericDomainEventEntry.getType(),
                genericDomainEventEntry.getAggregateIdentifier(),
                genericDomainEventEntry.getSequenceNumber(), null,
                genericDomainEventEntry::getTimestamp);
    }
}
