package org.axonframework.extensions.reactor.messaging.unitofwork;

import java.util.LinkedList;

/**
 * Type Alias for list of Unit of Works
 *
 * @author Stefan Dragisic
 */
public class ExecutionContext extends LinkedList<ReactiveUnitOfWork<?>> { }
