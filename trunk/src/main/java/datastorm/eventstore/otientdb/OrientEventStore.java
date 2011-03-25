package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.eventstore.EventSerializer;
import org.axonframework.eventstore.EventStore;
import org.axonframework.eventstore.EventStoreException;
import org.axonframework.eventstore.XStreamEventSerializer;

/**
 * An {@link EventStore} implementation that uses OrientDB to store DomainEvents in a database.
 *
 * @author EniSh
 */
public class OrientEventStore implements EventStore {
    private final EventSerializer eventSerializer;
    private ODatabaseDocumentTx database;

    public OrientEventStore() {
        eventSerializer = new XStreamEventSerializer();
    }

    public OrientEventStore(EventSerializer eventSerializer) {
        this.eventSerializer = eventSerializer;
    }

    /**
     * {@inheritDoc}
     */
    public void appendEvents(String type, DomainEventStream domainEventStream) {

        while (domainEventStream.hasNext()) {
            DomainEvent event = domainEventStream.next();
            ODocument eventDocument = new ODocument(database, type + ":" + event.getAggregateIdentifier());
            eventDocument.field("sequenceNumber", event.getSequenceNumber());
            eventDocument.field("timestamp", event.getTimestamp());
            eventDocument.field("body", eventSerializer.serialize(event));
            eventDocument.save();
        }
    }

    /**
     * {@inheritDoc}
     */
    public DomainEventStream readEvents(String type, AggregateIdentifier aggregateIdentifier) {
        try {
            //TODO Check for shapshots
            return new ORecordEventStream(database.browseCluster(type + ":" + aggregateIdentifier));
        } catch (IllegalArgumentException e) {
            throw new EventStoreException(
                    String.format("An error occurred while trying to read events "
                            + "for aggregate type [%s] with identifier [%s]",
                            type,
                            aggregateIdentifier.toString()), e);
        }
    }

    private final class ORecordEventStream implements DomainEventStream {
        private final ORecordIteratorCluster<ODocument> clusterIterator;

        private ORecordEventStream(ORecordIteratorCluster<ODocument> clusterIterator) {
            this.clusterIterator = clusterIterator;
        }

        public boolean hasNext() {
            return clusterIterator.hasNext();
        }

        public DomainEvent next() {
            return document2DomainView(clusterIterator.next());
        }

        public DomainEvent peek() {
            return document2DomainView((ODocument) clusterIterator.current());
        }

        private DomainEvent document2DomainView(ODocument document) {
            return eventSerializer.deserialize(document.<byte[]>field("body"));
        }
    }
}
