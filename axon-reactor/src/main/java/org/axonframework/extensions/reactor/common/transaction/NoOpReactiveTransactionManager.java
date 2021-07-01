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

package org.axonframework.extensions.reactor.common.transaction;

import reactor.core.publisher.Mono;

/**
 * TransactionManager implementation that does nothing. It's a placeholder implementation for the cases where no
 * special transaction management is required.
 *
 * @author Stefan Dragisic
 */
enum NoTransactionManager implements ReactiveAxonTransactionManager {

    /**
     * Singleton instance of the TransactionManager
     */
    INSTANCE;

    /**
     * Returns the singleton instance of this TransactionManager
     *
     * @return the singleton instance of this TransactionManager
     */
    public static ReactiveAxonTransactionManager instance() {
        return INSTANCE;
    }

    @Override
    public Mono<ReactiveAxonTransaction> startTransaction() {
        return Mono.just(TRANSACTION);
    }

    private static final ReactiveAxonTransaction TRANSACTION = new ReactiveAxonTransaction() {
        @Override
        public Mono<Void> commit() {
            //no op
            return Mono.empty();
        }

        @Override
        public Mono<Void> rollback() {
            //no op
            return Mono.empty();
        }
    };
}