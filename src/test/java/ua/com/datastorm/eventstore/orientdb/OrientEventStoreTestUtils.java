package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.axonframework.domain.*;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Utility class that is used to make OrientDb EventStore tests shorter and much more readable by leveraging
 * static imports. Contains assertions and factory methods.
 *
 * @author Andrey Lomakin
 */
abstract class OrientEventStoreTestUtils {

    /**
     * Generates list of {@link SimpleDomainEvent}s by passed in sequence numbers and aggregate IDs.
     * Each sequence number and Aggregate identifier are mapped one by one. {@link StringAggregateIdentifier} will
     * be created from passed in Aggregate IDs. {@link ua.com.datastorm.eventstore.orientdb.SimpleDomainEvent#getValue()}
     * property will be auto generated.
     *
     * @param sequenceNumbers Array of sequence numbers.
     * @param ids             Array of Aggregate IDs.
     * @return List of {@link SimpleDomainEvent}s created from passed in data.
     */
    public static List<SimpleDomainEvent> createSimpleDomainEvents(int sequenceNumbers[], String[] ids) {
        if (sequenceNumbers.length != ids.length) {
            throw new IllegalArgumentException("Amount of sequence numbers should be equal to" +
                    " amount of aggregate IDs");
        }
        final List<SimpleDomainEvent> domainEvents = new ArrayList<SimpleDomainEvent>(ids.length);
        for (int i = 0; i < ids.length; i++) {
            domainEvents.add(new SimpleDomainEvent(sequenceNumbers[i],
                    new StringAggregateIdentifier(ids[i]),
                    "val" + i + "-" + ids[i]));
        }
        return domainEvents;
    }

    /**
     * Converts list of {@link DomainEvent}s into {@link DomainEventStream}.
     *
     * @param domainEvents List of {@link DomainEvent}s.
     * @return {@link DomainEventStream} that contains passed in DomainEvents.
     */
    public static DomainEventStream stream(List<? extends DomainEvent> domainEvents) {
        return new SimpleDomainEventStream(domainEvents);
    }

    /**
     * Checks that {@link DomainEventStream} contains only DomainEvents form the passed in list.
     * equals method will be used to check equality.
     *
     * @param appendedEvents List of DomainEvents that should be contained into the stream.
     * @param readEvents     {@link DomainEventStream} to be checked.
     */
    public static void assertDomainEventsEquality(List<? extends DomainEvent> appendedEvents,
                                                  DomainEventStream readEvents) {
        for (DomainEvent appendedEvent : appendedEvents) {
            assertTrue(readEvents.hasNext());
            final DomainEvent readEvent = readEvents.next();
            assertEquals(appendedEvent, readEvent);
        }
        assertFalse(readEvents.hasNext());
    }

    /**
     * Sort passed in list of {@link DomainEvent}s by sequence number.
     * Original list will be untouched.
     *
     * @param domainEvents List of {@link DomainEvent}s to be sorted.
     * @return Sorted copy of {@link DomainEvent}s list.
     */
    public static List<? extends DomainEvent> sortBySequenceNumber(List<? extends DomainEvent> domainEvents) {
        List<? extends DomainEvent> copiedEvents = new ArrayList<DomainEvent>(domainEvents);
        Collections.sort(copiedEvents, new Comparator<DomainEvent>() {
            @Override
            public int compare(DomainEvent eventOne, DomainEvent eventTwo) {
                return eventOne.getSequenceNumber().compareTo(eventTwo.getSequenceNumber());
            }
        });
        return copiedEvents;
    }

    /**
     * Creates {@link AggregateIdentifier} form its String presentation.
     *
     * @param id String presentation of {@link AggregateIdentifier}.
     * @return Instance of {@link AggregateIdentifier}.
     */
    public static AggregateIdentifier agId(String id) {
        return new StringAggregateIdentifier(id);
    }

    /**
     * Checks class definition of the document that presents {@link DomainEvent} instance in OrientDb.
     *
     * For class description look at {@link DomainEventEntry} JavaDoc.
     *
     * @param eventClass Document class to be checked.
     */
    public static void assertDomainEventSchema(OClass eventClass) {
        assertNotNull(eventClass);
        assertEquals(DomainEventEntry.DOMAIN_EVENT_CLASS, eventClass.getName());

        final OProperty aggregateTypeProperty = eventClass.getProperty("aggregateType");
        assertNotNull(aggregateTypeProperty);
        assertTrue(aggregateTypeProperty.isMandatory());
        assertTrue(aggregateTypeProperty.isNotNull());
        assertEquals(OType.STRING, aggregateTypeProperty.getType());

        final OProperty aggregateIdentifierProperty = eventClass.getProperty("aggregateIdentifier");
        assertNotNull(aggregateIdentifierProperty);
        assertTrue(aggregateIdentifierProperty.isMandatory());
        assertTrue(aggregateIdentifierProperty.isNotNull());
        assertEquals(OType.STRING, aggregateIdentifierProperty.getType());

        final OProperty sequenceNumberProperty = eventClass.getProperty("sequenceNumber");
        assertNotNull(sequenceNumberProperty);
        assertTrue(sequenceNumberProperty.isMandatory());
        assertTrue(sequenceNumberProperty.isNotNull());
        assertEquals(OType.LONG, sequenceNumberProperty.getType());

        final OProperty timestampProperty = eventClass.getProperty("timestamp");
        assertNotNull(timestampProperty);
        assertTrue(timestampProperty.isMandatory());
        assertTrue(timestampProperty.isNotNull());
        assertEquals("29", timestampProperty.getMin());
        assertEquals("29", timestampProperty.getMax());
        assertEquals(OType.STRING, timestampProperty.getType());

        final OProperty bodyProperty = eventClass.getProperty("body");
        assertNotNull(bodyProperty);
        assertTrue(bodyProperty.isMandatory());
        assertTrue(bodyProperty.isNotNull());
        assertEquals(OType.BINARY, bodyProperty.getType());
    }

    /**
     * Checks class definition of the document that presents Snapshot Event instance in OrientDb.
     *
     * For class description look at {@link SnapshotEventEntry} JavaDoc.
     *
     * @param eventClass Document class to be checked.
     */
    public static void assertSnapshotEventSchema(OClass eventClass) {
        assertNotNull(eventClass);
        assertEquals(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, eventClass.getName());

        final OProperty aggregateTypeProperty = eventClass.getProperty("aggregateType");
        assertNotNull(aggregateTypeProperty);
        assertTrue(aggregateTypeProperty.isMandatory());
        assertTrue(aggregateTypeProperty.isNotNull());
        assertEquals(OType.STRING, aggregateTypeProperty.getType());

        final OProperty aggregateIdentifierProperty = eventClass.getProperty("aggregateIdentifier");
        assertNotNull(aggregateIdentifierProperty);
        assertTrue(aggregateIdentifierProperty.isMandatory());
        assertTrue(aggregateIdentifierProperty.isNotNull());
        assertEquals(OType.STRING, aggregateIdentifierProperty.getType());

        final OProperty sequenceNumberProperty = eventClass.getProperty("sequenceNumber");
        assertNotNull(sequenceNumberProperty);
        assertTrue(sequenceNumberProperty.isMandatory());
        assertTrue(sequenceNumberProperty.isNotNull());
        assertEquals(OType.LONG, sequenceNumberProperty.getType());

        final OProperty timestampProperty = eventClass.getProperty("timestamp");
        assertNotNull(timestampProperty);
        assertTrue(timestampProperty.isMandatory());
        assertTrue(timestampProperty.isNotNull());
        assertEquals("29", timestampProperty.getMin());
        assertEquals("29", timestampProperty.getMax());
        assertEquals(OType.STRING, timestampProperty.getType());

        final OProperty bodyProperty = eventClass.getProperty("body");
        assertNotNull(bodyProperty);
        assertTrue(bodyProperty.isMandatory());
        assertTrue(bodyProperty.isNotNull());
        assertEquals(OType.BINARY, bodyProperty.getType());

        final OClass parent = eventClass.getSuperClass();
        assertDomainEventSchema(parent);
    }
}
