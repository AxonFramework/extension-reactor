package org.axonframework.extensions.reactor.eventstore.mappers;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.extensions.reactor.eventstore.utils.Tuple2;


/**
 * @author vtiwar27
 * @date 2020-11-03
 */

public class TrackedEventDataMapper {

    private final EventSchema eventSchema;
    private Class<?> dataType;

    public TrackedEventDataMapper(EventSchema eventSchema, Class<?> dataType) {
        this.eventSchema = eventSchema;
        this.dataType = dataType;
    }


    public Tuple2<Long, DomainEventData<?>> map(Row row, RowMetadata rowMetadata) {
        long globalSequence = row.get(eventSchema.globalIndexColumn(), Long.class);

        final GenericDomainEventEntry<?> domainEventEntry = new GenericDomainEventEntry<>(
                row.get(eventSchema.typeColumn(), String.class),
                row.get(eventSchema.aggregateIdentifierColumn(), String.class),
                row.get(eventSchema.sequenceNumberColumn(), Long.class),
                row.get(eventSchema.eventIdentifierColumn(), String.class),
                row.get(eventSchema.timestampColumn(), String.class),
                row.get(eventSchema.payloadTypeColumn(), String.class),
                row.get(eventSchema.eventIdentifierColumn(), String.class),
                readPayload(row, eventSchema.payloadColumn()),
                readPayload(row, eventSchema.metaDataColumn()));
        return new Tuple2<>(globalSequence, domainEventEntry);
    }

    private <T> T readPayload(Row row, String columnName) {
        return (T) row.get(columnName, this.dataType);
    }
}
