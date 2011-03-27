package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.SimpleDomainEventStream;
import org.axonframework.eventstore.EventSerializer;
import org.axonframework.eventstore.EventStore;
import org.axonframework.eventstore.EventStoreException;
import org.axonframework.eventstore.XStreamEventSerializer;

import java.util.Iterator;
import java.util.List;

/**
 * An {@link EventStore} implementation that uses OrientDB to store DomainEvents in a database.
 *
 * @author EniSh
 */
public class OrientEventStore implements EventStore {
    private final EventSerializer eventSerializer;

    private ODatabaseDocumentTx database;
    private ClusterResolver clusterResolver;

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

            String aggregateCluster = null;

            AggregateIdentifier aggregateIdentifier = event.getAggregateIdentifier();
            if(clusterResolver != null) {
                aggregateCluster = clusterResolver.resolveClusterForAggregate(type, aggregateIdentifier);
            }

            ODocument eventDocument = new ODocument(database, type);
            eventDocument.field("aggregateIdentifier", aggregateIdentifier.asString());
            eventDocument.field("sequenceNumber", event.getSequenceNumber());
            eventDocument.field("timestamp", event.getTimestamp());
            eventDocument.field("body", eventSerializer.serialize(event));

            eventDocument.save(aggregateCluster);
        }
    }

    /**
     * {@inheritDoc}
     */
    public DomainEventStream readEvents(String type, AggregateIdentifier aggregateIdentifier) {
        try {
            List<ODocument> queryResult =
                    database.query(new OSQLSynchQuery<ODocument>("select * from " + type +
                            " where aggregateIdentifier = '" + aggregateIdentifier.asString() +
                            "' order by sequenceNumber"));
            return new SimpleDomainEventStream(new DocumentEventIterator(queryResult.iterator()));
        } catch (IllegalArgumentException e) {
            throw new EventStoreException(
                    String.format("An error occurred while trying to read events "
                            + "for aggregate type [%s] with identifier [%s]",
                            type,
                            aggregateIdentifier.toString()), e);
        }
    }

    public void setDatabase(ODatabaseDocumentTx database) {
        this.database = database;
    }

    public void setClusterResolver(ClusterResolver clusterResolver) {
        this.clusterResolver = clusterResolver;
    }

    private final class DocumentEventIterator implements Iterator<DomainEvent>, Iterable<DomainEvent> {
        private final Iterator<ODocument> documentIterator;

        private DocumentEventIterator(Iterator<ODocument> classIterator) {
            this.documentIterator = classIterator;
        }

        public boolean hasNext() {
            return documentIterator.hasNext();
        }

        public DomainEvent next() {
            return document2DomainView(documentIterator.next());
        }

        public void remove() {
            throw new UnsupportedOperationException("Remove operation is not supported");
        }

        public Iterator<DomainEvent> iterator() {
            return this;
        }

        private DomainEvent document2DomainView(ODocument document) {
            return eventSerializer.deserialize(document.<byte[]>field("body"));
        }
    }
}
