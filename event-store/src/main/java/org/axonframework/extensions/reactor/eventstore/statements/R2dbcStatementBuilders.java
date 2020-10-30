package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

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
                + ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)");
        events.forEach(eventMessage -> {
            DomainEventMessage<?> event = asDomainEventMessage(eventMessage);
            SerializedObject<?> payload = event.serializePayload(serializer, dataType);
            SerializedObject<?> metaData = event.serializeMetaData(serializer, dataType);
            statement.bind("$1", event.getIdentifier());
            statement.bind("$2", event.getAggregateIdentifier());
            statement.bind("$3", event.getSequenceNumber());
            statement.bind("$4", event.getType());
            statement.bind("$5", event.getTimestamp());
            statement.bind("$6", payload.getType().getName());
            statement.bind("$7", "");
            statement.bind("$8", payload.getData());
            statement.bind("$9", metaData.getData());
            statement.add();
        });
        statement.fetchSize(1);
        return statement;
    }

    public static String getEventsStatement(EventSchema schema) {
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE " + schema
                        .aggregateIdentifierColumn() + " = $1 AND " + schema.sequenceNumberColumn() + " >= $2 AND "
                        + schema.sequenceNumberColumn() + " < $3 ORDER BY "
                        + schema.sequenceNumberColumn() + " ASC";
        return sql;
    }

    public static Statement fetchTrackedEventsStatement(Connection connection, EventSchema schema, long index) {
        final String sql =
                "SELECT min(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                        + schema.globalIndexColumn() + " > $1";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", index);
        return statement;
    }

    public static Statement readEventDataWithoutGaps(Connection connection, EventSchema schema,
                                                     long globalIndex, int batchSize) throws SQLException {
        final String sql = "SELECT "
                + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE ("
                + schema.globalIndexColumn() + " > $1 AND " + schema.globalIndexColumn()
                + " <= $2) ORDER BY " + schema.globalIndexColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", globalIndex);
        statement.bind("$2", globalIndex + batchSize);
        return statement;
    }

    public static Statement readEventDataWithGaps(Connection connection, EventSchema schema, long globalIndex,
                                                  int batchSize, List<Long> gaps) {
        final Integer gapSize = gaps.size();
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE ("
                        + schema.globalIndexColumn() + " > $1 AND " + schema.globalIndexColumn() + " <= $2) OR "
                        + schema.globalIndexColumn() + " IN (" + String.join(",", Collections.nCopies(gapSize, "?"))
                        + ") ORDER BY " + schema.globalIndexColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", globalIndex);
        statement.bind("$2", globalIndex + batchSize);
        for (int i = 0; i < gapSize; i++) {
            statement.bind(i + 3, gaps.get(i));
        }
        return statement;
    }


    public static String lastSequenceNumberFor(EventSchema schema) {
        return "SELECT max("
                + schema.sequenceNumberColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                + schema.aggregateIdentifierColumn() + " = $1";
    }


    public static String createTailToken(EventSchema schema) {
        final String sql = "SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable();
        return sql;
    }


    public static String createHeadToken(EventSchema schema) {
        final String sql = "SELECT max(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable();
        return sql;
    }

    public static String createTokenAt(EventSchema schema, Instant dateTime) {
        final String sql =
                "SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable() + " WHERE "
                        + schema.timestampColumn() + " >= $1";
        //statement.bind("$1", formatInstant(dateTime));
        return sql;
    }


    public static Statement createDomainEventTable(Connection connection,
                                                   EventSchema schema) {
        String sql = "CREATE TABLE IF NOT EXISTS " + schema.domainEventTable() + " (\n" +
                schema.globalIndexColumn() + " " + idColumnType() + " NOT NULL,\n" +
                schema.aggregateIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.sequenceNumberColumn() + " BIGINT NOT NULL,\n" +
                schema.typeColumn() + " VARCHAR(255),\n" +
                schema.eventIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.metaDataColumn() + " " + payloadType() + ",\n" +
                schema.payloadColumn() + " " + payloadType() + " NOT NULL,\n" +
                schema.payloadRevisionColumn() + " VARCHAR(255),\n" +
                schema.payloadTypeColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.timestampColumn() + " VARCHAR(255) NOT NULL,\n" +
                "PRIMARY KEY (" + schema.globalIndexColumn() + "),\n" +
                "UNIQUE (" + schema.aggregateIdentifierColumn() + ", " +
                schema.sequenceNumberColumn() + "),\n" +
                "UNIQUE (" + schema.eventIdentifierColumn() + ")\n" +
                ")";
        return connection.createStatement(sql);
    }


    public static PreparedStatement createSnapshotEventTable(java.sql.Connection connection,
                                                             EventSchema schema) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + schema.snapshotTable() + " (\n" +
                schema.aggregateIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.sequenceNumberColumn() + " BIGINT NOT NULL,\n" +
                schema.typeColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.eventIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.metaDataColumn() + " " + payloadType() + ",\n" +
                schema.payloadColumn() + " " + payloadType() + " NOT NULL,\n" +
                schema.payloadRevisionColumn() + " VARCHAR(255),\n" +
                schema.payloadTypeColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.timestampColumn() + " VARCHAR(255) NOT NULL,\n" +
                "PRIMARY KEY (" + schema.aggregateIdentifierColumn() + ", " +
                schema.sequenceNumberColumn() + "),\n" +
                "UNIQUE (" + schema.eventIdentifierColumn() + ")\n" +
                ")";
        return connection.prepareStatement(sql);
    }


    private static String idColumnType() {
        return "BIGSERIAL";
    }


    private static String payloadType() {
        return "bytea";
    }
}
