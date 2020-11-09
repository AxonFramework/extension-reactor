package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.eventhandling.EventUtils.asDomainEventMessage;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class R2dbcStatementBuilders {

    public static Statement getAppendEventStatement(Connection connection, EventSchema eventSchema, Class<?> dataType, List<? extends EventMessage<?>> events,
                                                    Serializer serializer, TimestampWriter timestampWriter) {
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


    public static Statement readEventsDataForAggregate(Connection connection, EventSchema schema, String aggregateIdentifier,
                                                       long firstSequenceNumber,
                                                       int batchSize) {
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE " + schema
                        .aggregateIdentifierColumn() + " = $1 AND " + schema.sequenceNumberColumn() + " >= $2 AND "
                        + schema.sequenceNumberColumn() + " < $3 ORDER BY "
                        + schema.sequenceNumberColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", aggregateIdentifier)
                .bind("$2", firstSequenceNumber)
                .bind("$3", (firstSequenceNumber + batchSize));
        return statement;
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
                                                     long globalIndex, int batchSize) {
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

    public static Statement cleanGaps(Connection connection, EventSchema schema, SortedSet<Long> gaps) {
        final String sql = "SELECT "
                + schema.globalIndexColumn() + ", " + schema.timestampColumn() + " FROM " + schema
                .domainEventTable() + " WHERE " + schema.globalIndexColumn() + " >= $1 AND " + schema
                .globalIndexColumn() + " <= $2";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", gaps.first());
        statement.bind("$2", gaps.last() + 1L);
        return statement;
    }


    public static Statement lastSequenceNumberFor(Connection connection, EventSchema schema, String aggregateIdentifier) {
        String sql = "SELECT max("
                + schema.sequenceNumberColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                + schema.aggregateIdentifierColumn() + " = $1";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", aggregateIdentifier);
        return statement;
    }


    public static Statement createTailToken(Connection connection, EventSchema schema) {
        return connection.createStatement("SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable());

    }


    public static Statement createHeadToken(Connection connection, EventSchema schema) {
        return connection.createStatement("SELECT max(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable());
    }

    public static Statement createTokenAt(Connection connection, EventSchema schema, Instant dateTime) {
        final Statement statement =
                connection.createStatement("SELECT min(" + schema.globalIndexColumn() + ") - 1 FROM " + schema.domainEventTable() + " WHERE "
                        + schema.timestampColumn() + " >= $1");
        statement.bind("$1", formatInstant(dateTime));
        return statement;
    }

    public static Statement readSnapshotData(Connection connection, EventSchema schema, String identifier) {
        final String sql = "SELECT "
                + schema.domainEventFields() + " FROM " + schema.snapshotTable() + " WHERE "
                + schema.aggregateIdentifierColumn() + " = $1 ORDER BY " + schema.sequenceNumberColumn()
                + " DESC";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", identifier);
        return statement;
    }

    public static Statement deleteSnapshots(Connection connection, EventSchema schema,
                                            String aggregateIdentifier, long sequenceNumber) {
        final String sql = "DELETE FROM " + schema.snapshotTable() + " WHERE " + schema.aggregateIdentifierColumn()
                + " = $1 AND " + schema.sequenceNumberColumn() + " < $2";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", aggregateIdentifier);
        statement.bind("$2", sequenceNumber);
        return statement;
    }


    public static Statement fetchTrackedEvents(Connection connection, EventSchema schema, long index) {
        final String sql =
                "SELECT min(" + schema.globalIndexColumn() + ") FROM " + schema.domainEventTable() + " WHERE "
                        + schema.globalIndexColumn() + " > $1";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", index);
        return statement;
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


    public static Statement createSnapshotEventTable(Connection connection,
                                                     EventSchema schema) {
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
        return connection.createStatement(sql);
    }

    public static Statement appendSnapshot(Connection connection,
                                           EventSchema schema,
                                           Class<?> dataType,
                                           DomainEventMessage<?> snapshot,
                                           Serializer serializer,
                                           TimestampWriter timestampWriter) {
        final String sql = "INSERT INTO "
                + schema.snapshotTable() + " (" + schema.domainEventFields() + ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)";
        Statement statement = connection.createStatement(sql);
        SerializedObject<?> payload = snapshot.serializePayload(serializer, dataType);
        SerializedObject<?> metaData = snapshot.serializeMetaData(serializer, dataType);
        statement.bind("$1", snapshot.getIdentifier());
        statement.bind("$2", snapshot.getAggregateIdentifier());
        statement.bind("$3", snapshot.getSequenceNumber());
        statement.bind("$4", snapshot.getType());
        statement.bind("$5", snapshot.getTimestamp());
        //timestampWriter.writeTimestamp(statement, 5, snapshot.getTimestamp());
        statement.bind("$6", payload.getType().getName());
        statement.bind("$7", "");
        statement.bind("$8", payload.getData());
        statement.bind("$9", metaData.getData());
        return statement;
    }


    private static String idColumnType() {
        return "BIGSERIAL";
    }


    private static String payloadType() {
        return "bytea";
    }
}
