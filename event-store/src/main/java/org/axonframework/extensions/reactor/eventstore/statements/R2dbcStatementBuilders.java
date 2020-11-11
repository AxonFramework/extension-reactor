package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.eventhandling.EventUtils.asDomainEventMessage;

/**
 * @author vtiwar27
 * @date 2020-10-15
 */
public class R2dbcStatementBuilders {

    public static Statement getAppendEventStatement(Connection connection, EventSchema eventSchema, Class<?> dataType, List<? extends EventMessage<?>> events,
                                                    Serializer serializer) {
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
            statement.bind("$5", formatInstant(event.getTimestamp()));
            statement.bind("$6", payload.getType().getName());
            if (StringUtils.hasText(payload.getType().getRevision())) {
                statement.bind("$7", payload.getType().getRevision());
            } else {
                statement.bindNull("$7", String.class);
            }
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
        final int gapSize = gaps.size();
        final String sql =
                "SELECT " + schema.trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE ("
                        + schema.globalIndexColumn() + " > $1 AND " + schema.globalIndexColumn() + " <= $2) OR "
                        + schema.globalIndexColumn() + " IN (" + IntStream.range(3, gapSize + 3).boxed().map(index -> String.format("$%d", index)).collect(Collectors.joining(","))
                        + ") ORDER BY " + schema.globalIndexColumn() + " ASC";
        Statement statement = connection.createStatement(sql);
        statement.bind("$1", globalIndex);
        statement.bind("$2", globalIndex + batchSize);
        for (int i = 0; i < gapSize; i++) {
            statement.bind("$" + (i + 3), gaps.get(i));
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


    public static Statement appendSnapshot(Connection connection,
                                           EventSchema schema,
                                           Class<?> dataType,
                                           DomainEventMessage<?> snapshot,
                                           Serializer serializer) {
        final String sql = "INSERT INTO "
                + schema.snapshotTable() + " (" + schema.domainEventFields() + ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)";
        Statement statement = connection.createStatement(sql);
        SerializedObject<?> payload = snapshot.serializePayload(serializer, dataType);
        SerializedObject<?> metaData = snapshot.serializeMetaData(serializer, dataType);
        statement.bind("$1", snapshot.getIdentifier());
        statement.bind("$2", snapshot.getAggregateIdentifier());
        statement.bind("$3", snapshot.getSequenceNumber());
        statement.bind("$4", snapshot.getType());
        statement.bind("$5", formatInstant(snapshot.getTimestamp()));
        statement.bind("$6", payload.getType().getName());
        if (StringUtils.hasText(payload.getType().getRevision())) {
            statement.bind("$7", payload.getType().getRevision());
        } else {
            statement.bindNull("$7", String.class);
        }
        statement.bind("$8", payload.getData());
        statement.bind("$9", metaData.getData());
        return statement;
    }


}
