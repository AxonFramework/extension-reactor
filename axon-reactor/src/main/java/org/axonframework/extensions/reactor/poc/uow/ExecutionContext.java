package org.axonframework.extensions.reactor.poc.uow;

import java.util.LinkedList;

/**
 * Type Alias for list of Unit of Works
 *
 * @author Stefan Dragisic
 */
public class ExecutionContext extends LinkedList<ReactiveUnitOfWork<?>> { }
