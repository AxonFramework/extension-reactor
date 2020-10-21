package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;

import java.sql.SQLException;
import java.time.Instant;

/**
 * @author vtiwar27
 * @date 2020-10-05
 */


@FunctionalInterface
public interface R2dbcAppendEventStatementBuilder {

    Statement build(Connection connection, EventSchema schema, Instant dateTime);
}
