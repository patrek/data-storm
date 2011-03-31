package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.axonframework.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.*;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests only  storing and reading events for default cluster.
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

        final List<SimpleDomainEvent> domainEvents = createSimpleDomainEvents(new int[] {1, 2},
                new String[]  {"1", "1"});

        orientEventStore.appendEvents("Simple", stream(domainEvents));

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", agId("1"));

        assertDomainEventsEquality(domainEvents, readEventStream);
    }

    @Test
    public void testEventsSorting() throws Exception {

        final List<SimpleDomainEvent> domainEvents = createSimpleDomainEvents(
                new int[] { 3, 1, 5, 9, 2, 4, 6, 8, 7 },
                new String[] {"1", "1", "1", "1", "1", "1", "1", "1", "1"}
        );

        orientEventStore.appendEvents("Simple", stream(domainEvents));

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", agId("1"));

        assertDomainEventsEquality(sortBySequenceNumber(domainEvents), readEventStream);
    }

    @Test
    public void testEventsFromDifferentTypesWithSameId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[] {1, 2},
                new String[] {"1", "1"});

        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[] {1, 2},
                new String[] {"1", "1"});

        orientEventStore.appendEvents("DocOne", stream(domainEventsDocOne));
        orientEventStore.appendEvents("DocTwo", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("DocTwo", agId("1"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("DocOne", agId("1"));


        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }

    @Test
    public void testEventsFromDifferentTypesWithDiffId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[] {1, 2},
                  new String[] {"1", "1"});
        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[] {1, 2},
                  new String[] {"2", "2"});

        orientEventStore.appendEvents("DocOne", stream(domainEventsDocOne));
        orientEventStore.appendEvents("DocTwo", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("DocTwo", agId("2"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("DocOne", agId("1"));


        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }

    @Test
    public void testEventsWithDiffId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[] {1, 2},
                  new String[] {"1", "1"});
        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[] {1, 2},
                  new String[] {"2", "2"});

        orientEventStore.appendEvents("Doc", stream(domainEventsDocOne));
        orientEventStore.appendEvents("Doc", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("Doc", agId("2"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("Doc", agId("1"));

        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }

}
