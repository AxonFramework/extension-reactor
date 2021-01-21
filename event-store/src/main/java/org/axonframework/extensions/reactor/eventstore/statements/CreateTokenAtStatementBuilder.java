/*
 * Copyright (c) 2010-2020. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.reactor.eventstore.statements;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.eventsourcing.eventstore.jdbc.JdbcEventStorageEngine;

import java.time.Instant;

/**
 * Contract which defines how to build a PreparedStatement for use on {@link JdbcEventStorageEngine#createTokenAt(Instant)}
 *
 * @author Lucas Campos
 * @since 4.3
 */
@FunctionalInterface
public interface CreateTokenAtStatementBuilder {

    /**
     * Creates a statement to be used at {@link JdbcEventStorageEngine#createTokenAt(Instant)}.
     *
     * @param connection The connection to the database.
     * @param schema     The EventSchema to be used
     * @param dateTime   The dateTime where the token will be created.
     * @return the newly created {@link Statement}.
     */
    Statement build(Connection connection, EventSchema schema, Instant dateTime);
}
