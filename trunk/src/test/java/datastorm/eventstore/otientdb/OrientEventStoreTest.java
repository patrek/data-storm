package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.axonframework.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.java2d.pipe.SpanShapeRenderer;

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
    private ODatabaseDocumentTx database;
    private OrientEventStore orientEventStore;

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
