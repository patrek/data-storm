package datastorm.eventstore.otientdb;

import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.eventstore.EventStore;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * An {@link EventStore} implementation that uses OrientDB to store DomainEvents in a database.
 *
 * @author EniSh
 */
public class OrientEventStore implements EventStore {
    /**
     * {@inheritDoc}
     */
    public void appendEvents(String s, DomainEventStream domainEventStream) {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    public DomainEventStream readEvents(String s, AggregateIdentifier aggregateIdentifier) {
        throw new NotImplementedException();
    }
}
