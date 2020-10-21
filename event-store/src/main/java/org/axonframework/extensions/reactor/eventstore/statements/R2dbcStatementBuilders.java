package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.eventhandling.EventUtils.asDomainEventMessage;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class R2dbcStatementBuilders {

    public static Statement getAppendEventStatement(Connection connection, List<? extends EventMessage<?>> events, EventSchema eventSchema,
                                                    Serializer serializer, Class<?> dataType) {
        final Statement statement = connection.createStatement("INSERT INTO " +
                eventSchema.domainEventTable() + " (" + eventSchema.domainEventFields()
                + ") VALUES (?,?,?,?,?,?,?,?,?)");
        events.forEach(eventMessage -> {
            DomainEventMessage<?> event = asDomainEventMessage(eventMessage);
            SerializedObject<?> payload = event.serializePayload(serializer, dataType);
            SerializedObject<?> metaData = event.serializeMetaData(serializer, dataType);
            statement.bind(1, event.getIdentifier());
            statement.bind(2, event.getAggregateIdentifier());
            statement.bind(3, event.getSequenceNumber());
            statement.bind(4, event.getType());
            statement.bind(5, event.getTimestamp());
            statement.bind(6, payload.getType().getName());
            statement.bind(7, payload.getType().getRevision());
            statement.bind(8, payload.getData());
            statement.bind(9, metaData.getData());
            statement.add();
        });

        return statement;
    }

    public static Statement getEventsStatement(Connection connection,
                                               EventSchema schema, int batchSize, String aggregateIdentifier, long firstSequenceNumber) {
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE " + schema
                        .aggregateIdentifierColumn() + " = ? AND " + schema.sequenceNumberColumn() + " >= ? AND "
                        + schema.sequenceNumberColumn() + " < ? ORDER BY "
                        + schema.sequenceNumberColumn() + " ASC";
        final Statement statement = connection.createStatement(sql);
        statement.bind(1, aggregateIdentifier);
        statement.bind(2, firstSequenceNumber);
        statement.bind(3, (firstSequenceNumber + batchSize));
        return statement;
    }

    public static Statement fetchTrackedEventsStatement(Connection connection, EventSchema schema, long index) {
        final String sql =
                "SELECT min(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                        + schema.globalIndexColumn() + " > ?";
        Statement statement = connection.createStatement(sql);
        statement.bind(1, index);
        return statement;
    }

    public static Statement readEventDataWithoutGaps(Connection connection, EventSchema schema,
                                                     long globalIndex, int batchSize) throws SQLException {
        final String sql = "SELECT "
                + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE ("
                + schema.globalIndexColumn() + " > ? AND " + schema.globalIndexColumn()
                + " <= ?) ORDER BY " + schema.globalIndexColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind(1, globalIndex);
        statement.bind(2, globalIndex + batchSize);
        return statement;
    }

    public static Statement readEventDataWithGaps(Connection connection, EventSchema schema, long globalIndex,
                                                  int batchSize, List<Long> gaps) {
        final Integer gapSize = gaps.size();
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE ("
                        + schema.globalIndexColumn() + " > ? AND " + schema.globalIndexColumn() + " <= ?) OR "
                        + schema.globalIndexColumn() + " IN (" + String.join(",", Collections.nCopies(gapSize, "?"))
                        + ") ORDER BY " + schema.globalIndexColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind(1, globalIndex);
        statement.bind(2, globalIndex + batchSize);
        for (int i = 0; i < gapSize; i++) {
            statement.bind(i + 3, gaps.get(i));
        }
        return statement;
    }


    public static Statement lastSequenceNumberFor(Connection connection, EventSchema schema,
                                                  String aggregateIdentifier) {
        final String sql = "SELECT max("
                + schema.sequenceNumberColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                + schema.aggregateIdentifierColumn() + " = ?";
        Statement statement = connection.createStatement(sql);
        statement.bind(1, aggregateIdentifier);
        return statement;
    }


    public static Statement createTailToken(Connection connection, EventSchema schema) {
        final String sql = "SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable();
        return connection.createStatement(sql);
    }


    public static Statement createHeadToken(Connection connection, EventSchema schema) {
        final String sql = "SELECT max(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable();
        return connection.createStatement(sql);
    }

    public static Statement createTokenAt(Connection connection, EventSchema schema, Instant dateTime) {
        final String sql =
                "SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable() + " WHERE "
                        + schema.timestampColumn() + " >= ?";
        Statement statement = connection.createStatement(sql);
        statement.bind(1, formatInstant(dateTime));
        return statement;
    }
}
