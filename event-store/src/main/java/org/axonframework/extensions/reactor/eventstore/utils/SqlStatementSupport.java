package org.axonframework.extensions.reactor.eventstore.utils;

import io.r2dbc.spi.Statement;

/**
 * @author vtiwar27
 * @date 2020-11-16
 */
public class SqlStatementSupport {

    public static void bindNullable(Statement statement, String binding, Object value, Class clazz) {
        if (value == null) {
            statement.bindNull(binding, clazz);
        } else {
            statement.bind(binding, value);
        }
    }

    public static String formatPlaceHolder(String index, String placeHolder) {
        return String.format("%s%s", placeHolder, index);
    }


}
