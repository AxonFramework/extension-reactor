/*
 * Copyright (c) 2010-2016. Axon Framework
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.reactor.poc.uow;

import org.axonframework.common.Assert;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Mono;

/**
 * TODO DOC
 * @author Stefan Dragisic
 */
public class ReactiveSpringTransactionManager implements ReactiveAxonTransactionManager {

    private final ReactiveTransactionManager transactionManager;
    private final TransactionDefinition transactionDefinition;

    /**
     * @param transactionManager    The transaction manager to use
     * @param transactionDefinition The definition for transactions to create
     */
    public ReactiveSpringTransactionManager(ReactiveTransactionManager transactionManager,
                                                TransactionDefinition transactionDefinition) {
        Assert.notNull(transactionManager, () -> "transactionManager may not be null");
        this.transactionManager = transactionManager;
        this.transactionDefinition = transactionDefinition;
    }

    /**
     * Initializes the SpringTransactionManager with the given {@code transactionManager} and the default
     * transaction definition.
     *
     * @param transactionManager the transaction manager to use
     */
    public ReactiveSpringTransactionManager(ReactiveTransactionManager transactionManager) {
        this(transactionManager, new DefaultTransactionDefinition());
    }

    @Override
    public Mono<ReactiveAxonTransaction> startTransaction() {
        return transactionManager.getReactiveTransaction(transactionDefinition)
                .map(status ->
                        new ReactiveAxonTransaction() {
                            @Override
                            public Mono<Void> commit() {
                                return commitTransaction(status);
                            }

                            @Override
                            public Mono<Void> rollback() {
                                return rollbackTransaction(status);
                            }
                        }
                );
    }

    /**
     * Commits the transaction with given {@code status} if the transaction is new and not completed.
     *
     * @param status The status of the transaction to commit
     */
    protected Mono<Void> commitTransaction(ReactiveTransaction status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            return transactionManager.commit(status);
        }
        else {
            return Mono.empty();
        }
    }

    /**
     * Rolls back the transaction with given {@code status} if the transaction is new and not completed.
     *
     * @param status The status of the transaction to roll back
     */
    protected Mono<Void> rollbackTransaction(ReactiveTransaction status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            return transactionManager.rollback(status);
        } else {
            return Mono.empty();
        }
    }
}
