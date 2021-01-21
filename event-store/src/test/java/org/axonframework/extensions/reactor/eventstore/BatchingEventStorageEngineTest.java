package org.axonframework.extensions.reactor.eventstore;

import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

import static org.axonframework.extensions.reactor.eventstore.utils.EventStoreTestUtils.AGGREGATE;
import static org.axonframework.extensions.reactor.eventstore.utils.EventStoreTestUtils.createEvents;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Transactional
public abstract class BatchingEventStorageEngineTest extends AbstractEventStorageEngineTest {

    private BatchingEventStorageEngine testSubject;

    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void testLoad_LargeAmountOfEvents() {
        int eventCount = testSubject.batchSize() + 10;
        testSubject.appendEvents(createEvents(eventCount));
        assertEquals(eventCount, testSubject.readEvents(AGGREGATE).asStream().count());
        assertEquals(eventCount - 1,
                     testSubject.readEvents(AGGREGATE).asStream().reduce((a, b) -> b).get().getSequenceNumber());
    }

    protected void setTestSubject(BatchingEventStorageEngine testSubject) {
        super.setTestSubject(this.testSubject = testSubject);
    }

}