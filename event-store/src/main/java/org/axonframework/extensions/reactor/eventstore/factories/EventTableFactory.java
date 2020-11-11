package org.axonframework.extensions.reactor.eventstore.factories;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;


/**
 * @author vtiwar27
 * @date 2020-11-11
 */
public interface EventTableFactory {
    Statement createDomainEventTable(Connection connection, EventSchema schema);

    Statement createSnapshotEventTable(Connection connection, EventSchema schema);
}
