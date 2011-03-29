package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.axonframework.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test case for {@link OrientEventStore}.
 *
 * @author EniSh
 */
public class OrientEventStoreTest {
    protected ODatabaseDocumentTx database;
    protected OrientEventStore orientEventStore;

    @Before
    public void setUp() throws Exception {
        database = new ODatabaseDocumentTx("memory:default");
        database.create();
        orientEventStore = new OrientEventStore();
        orientEventStore.setDatabase(database);
    }

    @After
    public void tearDown() throws Exception {
        database.delete();
    }

    @Test
    public void testBasicEventsStoring() throws Exception {
        final SimpleDomainEvent eventOne = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("1"), "val1");
        final SimpleDomainEvent eventTwo = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("1"), "val2");

        final DomainEventStream domainEventStream = new SimpleDomainEventStream(eventTwo, eventOne);

        orientEventStore.appendEvents("Simple", domainEventStream);

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", new StringAggregateIdentifier("1"));

        assertTrue(readEventStream.hasNext());

        final SimpleDomainEvent readEventOne = (SimpleDomainEvent)readEventStream.next();
        assertEquals((Long)1L, readEventOne.getSequenceNumber());
        assertEquals("1", readEventOne.getAggregateIdentifier().asString());
        assertEquals("val1", readEventOne.getValue());

        assertTrue(readEventStream.hasNext());

        final SimpleDomainEvent readEventTwo = (SimpleDomainEvent)readEventStream.next();
        assertEquals((Long)2L, readEventTwo.getSequenceNumber());
        assertEquals("1", readEventTwo.getAggregateIdentifier().asString());
        assertEquals("val2", readEventTwo.getValue());

        assertFalse(readEventStream.hasNext());
    }

    @Test
    public void testEventsSorting() throws Exception {
        final long sequenceNumbers[] = new long[] {
                3L, 1L, 5L, 9L, 2L, 4L, 6L, 8L, 7L
        };

        final AggregateIdentifier aggregateIdentifier = new StringAggregateIdentifier("1");

        final List<DomainEvent> domainEvents = new ArrayList<DomainEvent>();

        for(int i = 0; i < sequenceNumbers.length; i++) {
            domainEvents.add(new SimpleDomainEvent(sequenceNumbers[i], aggregateIdentifier, "val" + i));
        }


        final DomainEventStream domainEventStream = new SimpleDomainEventStream(domainEvents);

        orientEventStore.appendEvents("Simple", domainEventStream);

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", new StringAggregateIdentifier("1"));

        int i = 0;
        while (readEventStream.hasNext()) {
            i++;
            final DomainEvent domainEvent = readEventStream.next();
            assertEquals(Long.valueOf(i), domainEvent.getSequenceNumber() );

        }

        assertEquals(sequenceNumbers.length, i);
    }

    @Test
    public void testEventsFromDifferentTypesWithSameId() {
        final SimpleDomainEvent eventOneDocOne = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("1"), "val1doc1");
        final SimpleDomainEvent eventTwoDocOne = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("1"), "val2doc1");

        final SimpleDomainEvent eventOneDocTwo = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("1"), "val1doc2");
        final SimpleDomainEvent eventTwoDocTwo = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("1"), "val2doc2");

        final DomainEventStream domainEventStreamDocOne = new SimpleDomainEventStream(eventTwoDocOne,
                eventOneDocOne);

        final DomainEventStream domainEventStreamDocTwo = new SimpleDomainEventStream(eventOneDocTwo,
                eventTwoDocTwo);

        orientEventStore.appendEvents("DocOne", domainEventStreamDocOne);
        orientEventStore.appendEvents("DocTwo", domainEventStreamDocTwo);

        DomainEventStream readEventStreamDocTwo =
                orientEventStore.readEvents("DocTwo", new StringAggregateIdentifier("1"));

        DomainEventStream readEventStreamDocOne =
                orientEventStore.readEvents("DocOne", new StringAggregateIdentifier("1"));


        assertTrue(readEventStreamDocOne.hasNext());
        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventOneDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)1L, readEventOneDocOne.getSequenceNumber());
        assertEquals("1", readEventOneDocOne.getAggregateIdentifier().asString());
        assertEquals("val1doc1", readEventOneDocOne.getValue());

        assertTrue(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventTwoDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)2L, readEventTwoDocOne.getSequenceNumber());
        assertEquals("1", readEventTwoDocOne.getAggregateIdentifier().asString());
        assertEquals("val2doc1", readEventTwoDocOne.getValue());

        assertFalse(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventOneDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)1L, readEventOneDocTwo.getSequenceNumber());
        assertEquals("1", readEventOneDocTwo.getAggregateIdentifier().asString());
        assertEquals("val1doc2", readEventOneDocTwo.getValue());

        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventTwoDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)2L, readEventTwoDocTwo.getSequenceNumber());
        assertEquals("1", readEventTwoDocTwo.getAggregateIdentifier().asString());
        assertEquals("val2doc2", readEventTwoDocTwo.getValue());

        assertFalse(readEventStreamDocTwo.hasNext());
    }

    @Test
    public void testEventsFromDifferentTypesWithDiffId() {
        final SimpleDomainEvent eventOneDocOne = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("1"), "val1doc1");
        final SimpleDomainEvent eventTwoDocOne = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("1"), "val2doc1");

        final SimpleDomainEvent eventOneDocTwo = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("2"), "val1doc2");
        final SimpleDomainEvent eventTwoDocTwo = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("2"), "val2doc2");

        final DomainEventStream domainEventStreamDocOne = new SimpleDomainEventStream(eventTwoDocOne,
                eventOneDocOne);

        final DomainEventStream domainEventStreamDocTwo = new SimpleDomainEventStream(eventOneDocTwo,
                eventTwoDocTwo);

        orientEventStore.appendEvents("DocOne", domainEventStreamDocOne);
        orientEventStore.appendEvents("DocTwo", domainEventStreamDocTwo);

        DomainEventStream readEventStreamDocTwo =
                orientEventStore.readEvents("DocTwo", new StringAggregateIdentifier("2"));

        DomainEventStream readEventStreamDocOne =
                orientEventStore.readEvents("DocOne", new StringAggregateIdentifier("1"));


        assertTrue(readEventStreamDocOne.hasNext());
        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventOneDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)1L, readEventOneDocOne.getSequenceNumber());
        assertEquals("1", readEventOneDocOne.getAggregateIdentifier().asString());
        assertEquals("val1doc1", readEventOneDocOne.getValue());

        assertTrue(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventTwoDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)2L, readEventTwoDocOne.getSequenceNumber());
        assertEquals("1", readEventTwoDocOne.getAggregateIdentifier().asString());
        assertEquals("val2doc1", readEventTwoDocOne.getValue());

        assertFalse(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventOneDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)1L, readEventOneDocTwo.getSequenceNumber());
        assertEquals("2", readEventOneDocTwo.getAggregateIdentifier().asString());
        assertEquals("val1doc2", readEventOneDocTwo.getValue());

        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventTwoDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)2L, readEventTwoDocTwo.getSequenceNumber());
        assertEquals("2", readEventTwoDocTwo.getAggregateIdentifier().asString());
        assertEquals("val2doc2", readEventTwoDocTwo.getValue());

        assertFalse(readEventStreamDocTwo.hasNext());
    }

    @Test
    public void testEventsWithDiffId() {
        final SimpleDomainEvent eventOneDocOne = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("1"), "val1doc1");
        final SimpleDomainEvent eventTwoDocOne = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("1"), "val2doc1");

        final SimpleDomainEvent eventOneDocTwo = new SimpleDomainEvent(1,
                new StringAggregateIdentifier("2"), "val1doc2");
        final SimpleDomainEvent eventTwoDocTwo = new SimpleDomainEvent(2,
                new StringAggregateIdentifier("2"), "val2doc2");

        final DomainEventStream domainEventStreamDocOne = new SimpleDomainEventStream(eventTwoDocOne,
                eventOneDocOne);

        final DomainEventStream domainEventStreamDocTwo = new SimpleDomainEventStream(eventOneDocTwo,
                eventTwoDocTwo);

        orientEventStore.appendEvents("Doc", domainEventStreamDocOne);
        orientEventStore.appendEvents("Doc", domainEventStreamDocTwo);

        DomainEventStream readEventStreamDocTwo =
                orientEventStore.readEvents("Doc", new StringAggregateIdentifier("2"));

        DomainEventStream readEventStreamDocOne =
                orientEventStore.readEvents("Doc", new StringAggregateIdentifier("1"));


        assertTrue(readEventStreamDocOne.hasNext());
        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventOneDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)1L, readEventOneDocOne.getSequenceNumber());
        assertEquals("1", readEventOneDocOne.getAggregateIdentifier().asString());
        assertEquals("val1doc1", readEventOneDocOne.getValue());

        assertTrue(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventTwoDocOne = (SimpleDomainEvent)readEventStreamDocOne.next();
        assertEquals((Long)2L, readEventTwoDocOne.getSequenceNumber());
        assertEquals("1", readEventTwoDocOne.getAggregateIdentifier().asString());
        assertEquals("val2doc1", readEventTwoDocOne.getValue());

        assertFalse(readEventStreamDocOne.hasNext());

        final SimpleDomainEvent readEventOneDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)1L, readEventOneDocTwo.getSequenceNumber());
        assertEquals("2", readEventOneDocTwo.getAggregateIdentifier().asString());
        assertEquals("val1doc2", readEventOneDocTwo.getValue());

        assertTrue(readEventStreamDocTwo.hasNext());

        final SimpleDomainEvent readEventTwoDocTwo = (SimpleDomainEvent)readEventStreamDocTwo.next();
        assertEquals((Long)2L, readEventTwoDocTwo.getSequenceNumber());
        assertEquals("2", readEventTwoDocTwo.getAggregateIdentifier().asString());
        assertEquals("val2doc2", readEventTwoDocTwo.getValue());

        assertFalse(readEventStreamDocTwo.hasNext());
    }

    protected static final class SimpleDomainEvent extends DomainEvent {
        private String value;

        private SimpleDomainEvent(long sequenceNumber, AggregateIdentifier aggregateIdentifier, String value) {
            super(sequenceNumber, aggregateIdentifier);
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
