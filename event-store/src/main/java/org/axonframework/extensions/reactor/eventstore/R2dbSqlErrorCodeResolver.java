package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * @author vtiwar27
 * @date 2020-11-10
 */
public class R2dbSqlErrorCodeResolver implements PersistenceExceptionResolver {
    @Override
    public boolean isDuplicateKeyViolation(Exception exception) {
        return exception instanceof DataIntegrityViolationException;
    }
}
