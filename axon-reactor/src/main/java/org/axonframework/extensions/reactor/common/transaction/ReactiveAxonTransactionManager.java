package org.axonframework.extensions.reactor.common.transaction;

/*
 * Copyright (c) 2010-2018. Axon Framework
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


import org.axonframework.common.transaction.Transaction;
import reactor.core.publisher.Mono;

/**
 * Interface towards a mechanism that manages transactions
 * <p/>
 * Typically, this will involve opening database transactions or connecting to external systems.t
 *
 * @author Stefan Dragisic
 */
public interface ReactiveAxonTransactionManager {

    /**
     * Starts a transaction. The return value is the started transaction that can be committed or rolled back.
     *
     * @return The object representing the transaction
     */
    Mono<ReactiveAxonTransaction> startTransaction();

    /**
     * Executes the given {@code task} in a new {@link Transaction}. The transaction is committed when the task
     * completes normally, and rolled back when it throws an exception.
     *
     * @param task The task to execute
     */
    default Mono<Void> executeInTransaction(Mono<Void> task) {
        return startTransaction()
                .flatMap(transaction -> task
                        .then(transaction.commit())
                        .onErrorResume(t ->
                                transaction.rollback().then(Mono.error(t))
                        )
                );
    }

    /**
     * Invokes the given {@code supplier} in a transaction managed by the current TransactionManager. Upon completion
     * of the call, the transaction will be committed in the case of a regular return value, or rolled back in case an
     * exception occurred.
     * <p>
     * This method is an alternative to {@link #executeInTransaction(Mono)} in cases where a result needs to be
     * returned from the code to be executed transactionally.
     *
     * @param supplier The supplier of the value to return
     * @param <T>      The type of value to return
     * @return The value returned by the supplier
     */
    default <T> Mono<T> fetchInTransaction(Mono<T> supplier) {
        return startTransaction()
                .zipWith(supplier)
                .flatMap(transactionTuple -> transactionTuple.getT1()
                        .commit()
                        .thenReturn(transactionTuple.getT2())
                        .onErrorResume(t -> transactionTuple.getT1().rollback().then(Mono.error(t)))
                );
    }
}
