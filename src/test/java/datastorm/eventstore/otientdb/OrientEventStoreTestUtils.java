package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.axonframework.domain.*;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class OrientEventStoreTestUtils {
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

    public static DomainEventStream stream(List<? extends DomainEvent> domainEvents) {
        return new SimpleDomainEventStream(domainEvents);
    }

    public static void assertDomainEventsEquality(List<? extends DomainEvent> appendedEvents,
                                                  DomainEventStream readEvents) {
        for (DomainEvent appendedEvent : appendedEvents) {
            assertTrue(readEvents.hasNext());
            final DomainEvent readEvent = readEvents.next();
            assertEquals(appendedEvent, readEvent);
        }
        assertFalse(readEvents.hasNext());
    }

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

    public static AggregateIdentifier agId(String id) {
        return new StringAggregateIdentifier(id);
    }

    public static void assertClusterNames(Collection<String> beforeClusters,
                                          Collection<String> afterClusters, String[] clusterNames) {
        assertEquals(beforeClusters.size() + clusterNames.length, afterClusters.size());
        for(String clusterName : clusterNames) {
            assertFalse(beforeClusters.contains(clusterName));
            assertTrue(afterClusters.contains(clusterName));
        }
    }
}
