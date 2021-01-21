package org.axonframework.extensions.reactor.eventstore.mappers;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class DomainEventEntryMapper {

    private final EventSchema eventSchema;

    public DomainEventEntryMapper(EventSchema eventSchema) {
        this.eventSchema = eventSchema;
    }

    public DomainEventData<?> map(Row row, RowMetadata rowMetadata) {
        return new GenericDomainEventEntry<>(
                row.get(eventSchema.typeColumn(), String.class),
                row.get(eventSchema.aggregateIdentifierColumn(), String.class),
                row.get(eventSchema.sequenceNumberColumn(), Long.class),
                row.get(eventSchema.eventIdentifierColumn(), String.class),
                row.get(eventSchema.timestampColumn(), String.class),
                row.get(eventSchema.payloadTypeColumn(), String.class),
                row.get(eventSchema.eventIdentifierColumn(), String.class),
                row.get(eventSchema.payloadColumn()),
                row.get(eventSchema.metaDataColumn()));
    }
}
