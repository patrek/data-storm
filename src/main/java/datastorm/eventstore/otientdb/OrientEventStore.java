package datastorm.eventstore.otientdb;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.SimpleDomainEventStream;
import org.axonframework.eventstore.EventSerializer;
import org.axonframework.eventstore.EventStore;
import org.axonframework.eventstore.SnapshotEventStore;
import org.axonframework.eventstore.XStreamEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An {@link EventStore} implementation that uses Document oriented OrientDB to store DomainEvents in a database.
 * The actual DomainEvent is stored as byte array document field.
 * Other fields are used to store meta-data that allow quick finding of DomainEvents for a
 * specific aggregate in the correct order.
 * <p/>
 * The serializer is used to serialize the events is configurable.
 * By default, the {@link XStreamEventSerializer} is used.
 * <p/>
 * You can specify {@link ClusterResolver} that will be used to identify and create clusters where DomainEvents
 * will be stored. If Cluster Resolver is not specified default cluster will be used.
 *
 * @author EniSh
 */
public class OrientEventStore implements SnapshotEventStore {
    private static final Logger logger = LoggerFactory.getLogger(OrientEventStore.class);
    private final EventSerializer eventSerializer;

    private ODatabaseDocument database;
    private ClusterResolver clusterResolver;

    public OrientEventStore() {
        eventSerializer = new XStreamEventSerializer();
    }

    /**
     * Initialize EventStore with given serializer.
     *
     * @param eventSerializer Serializer that is used to store events
     */
    public OrientEventStore(EventSerializer eventSerializer) {
        this.eventSerializer = eventSerializer;
    }

    /**
     * {@inheritDoc}
     */
    public void appendEvents(String type, DomainEventStream domainEventStream) {
        while (domainEventStream.hasNext()) {
            final DomainEvent event = domainEventStream.next();
            final DomainEventEntry domainEventEntry = new DomainEventEntry(type, event, eventSerializer);
            storeEventEntry(domainEventEntry);
        }
    }

    /**
     * {@inheritDoc}
     */
    public DomainEventStream readEvents(String type, AggregateIdentifier aggregateIdentifier) {
        if (!database.getMetadata().getSchema().existsClass(DomainEventEntry.DOMAIN_EVENT_CLASS)) {
            return new SimpleDomainEventStream();
        }

        String aggregateCluster = null;
        if (clusterResolver != null) {
            aggregateCluster = clusterResolver.resolveClusterForAggregate(type, aggregateIdentifier);
        }

        final DomainEvent snapshotEvent = loadLastSnapshotEvent(type, aggregateIdentifier);

        String query;
        if (aggregateCluster != null) {
            query = "select * from cluster:" + aggregateCluster +
                    " where " + DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                    " and " + DomainEventEntry.AGGREGATE_TYPE_FIELD + " = '" + type + "'" +
                    " and (@class = '" + DomainEventEntry.DOMAIN_EVENT_CLASS + "' or @class = '" + SnapshotEventEntry.SNAPSHOT_EVENT_CLASS + "')";
        } else {
            query = "select * from " + DomainEventEntry.DOMAIN_EVENT_CLASS +
                    " where " + DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                    " and " + DomainEventEntry.AGGREGATE_TYPE_FIELD + " = '" + type + "'";
        }

        if (snapshotEvent != null) {
            query += " and ( " + DomainEventEntry.SEQUENCE_NUMBER_FIELD + " >= " + snapshotEvent.getSequenceNumber() + " )";
        }

        query += " order by " + DomainEventEntry.SEQUENCE_NUMBER_FIELD;

        final List<ODocument> queryResult =
                database.query(new OSQLSynchQuery<ODocument>(query));


        logger.debug("Query \"{}\" was performed and {} events were fetched.", query, queryResult.size());

        return new SimpleDomainEventStream(Collections2.transform(queryResult,
                new Function<ODocument, DomainEvent>() {
                    @Override
                    public DomainEvent apply(ODocument document) {
                        return eventSerializer.deserialize(document.<byte[]>field("body"));
                    }
                }));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendSnapshotEvent(String type, DomainEvent snapshotEvent) {
        final SnapshotEventEntry domainEventEntry = new SnapshotEventEntry(type, snapshotEvent, eventSerializer);
        storeEventEntry(domainEventEntry);
    }

    /**
     * Set OrientDB document oriented database instance that will be used to store DomainEvents.
     *
     * @param database OrientDB document oriented database instance.
     */
    public void setDatabase(ODatabaseDocument database) {
        this.database = database;
    }

    /**
     * Set {@link ClusterResolver} that will be used to identify in which cluster DomainEvents will be stored.
     *
     * @param clusterResolver {@link ClusterResolver} instance.
     */
    public void setClusterResolver(ClusterResolver clusterResolver) {
        this.clusterResolver = clusterResolver;
    }

    private DomainEvent loadLastSnapshotEvent(String aggregateType, AggregateIdentifier aggregateIdentifier) {
        if (!database.getMetadata().getSchema().existsClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS)) {
            return null;
        }

        String aggregateCluster = null;
        if (clusterResolver != null) {
            aggregateCluster = clusterResolver.resolveClusterForAggregate(aggregateType, aggregateIdentifier);
        }

        final String query;
        if (aggregateCluster != null) {
            query = "select * from cluster:" + aggregateCluster +
                    " where " + DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                    " and "+ DomainEventEntry.AGGREGATE_TYPE_FIELD + " = '" + aggregateType + "'" +
                    " and @class = '" + SnapshotEventEntry.SNAPSHOT_EVENT_CLASS +"'" +
                    " order by " + DomainEventEntry.SEQUENCE_NUMBER_FIELD + " desc limit 1";
        } else {
            query = "select * from " + SnapshotEventEntry.SNAPSHOT_EVENT_CLASS +
                    " where " + DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD +  " = '" + aggregateIdentifier.asString() + "'" +
                    " and " + DomainEventEntry.AGGREGATE_TYPE_FIELD + " = '" + aggregateType + "'" +
                    " order by "+ DomainEventEntry.SEQUENCE_NUMBER_FIELD + " desc limit 1";
        }

        final List<ODocument> queryResult =
                database.query(new OSQLSynchQuery<ODocument>(query));

        logger.debug("Query \"{}\" was performed and {} snapshot events were fetched.", query, queryResult.size());

        if (queryResult.isEmpty()) {
            return null;
        }
        return eventSerializer.deserialize(queryResult.get(0).<byte[]>field("body"));
    }

    private void storeEventEntry(DomainEventEntry domainEventEntry) {
        final DomainEvent event = domainEventEntry.getEvent();
        final String aggregateType = domainEventEntry.getAggregateType();

        String aggregateCluster = null;
        if (clusterResolver != null) {
            aggregateCluster = clusterResolver.resolveClusterForAggregate(aggregateType, event.getAggregateIdentifier());
        }

        final ODocument eventDocument = domainEventEntry.asDocument(database, aggregateCluster);

        if(aggregateCluster != null) {
            eventDocument.save(aggregateCluster);
        } else {
            eventDocument.save();
        }
        final OSchema schema = database.getMetadata().getSchema();
        schema.save();

        if (aggregateCluster != null) {
            logger.debug("Event for aggregate type \"{}\", id [{}] and sequence number {} was saved to the cluster \"{}\".",
                    new Object[]{aggregateType, event.getAggregateIdentifier().asString(), event.getSequenceNumber(),
                            aggregateCluster});
        } else {
            logger.debug("Event for aggregate type \"{}\", id [{}] and sequence number {} was saved to the default cluster.",
                    new Object[]{aggregateType, event.getSequenceNumber(), event.getAggregateIdentifier().asString()});
        }
    }
}
