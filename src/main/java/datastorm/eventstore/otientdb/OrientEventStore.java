package datastorm.eventstore.otientdb;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.sun.org.apache.bcel.internal.generic.D2F;
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
 *
 * You can specify {@link ClusterResolver} that will be used to identify and create clusters where DomainEvents
 * will be stored.
 *
 * @author EniSh
 */
public class OrientEventStore implements SnapshotEventStore {
    private static final Logger logger = LoggerFactory.getLogger(OrientEventStore.class);

    /**
     * Name of the document class that will be used to store DomainEvents.
     */
    public static final String DOMAIN_EVENT_CLASS = "DomainEvent";

    private final EventSerializer eventSerializer;

    private ODatabaseDocument database;
    private ClusterResolver clusterResolver;

    public OrientEventStore() {
        eventSerializer = new XStreamEventSerializer();
    }

    /**
     * Initialize EventStore with given serializer.
     *
     * @param eventSerializer  Serializer that is used to store events
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

            String aggregateCluster = null;

            final AggregateIdentifier aggregateIdentifier = event.getAggregateIdentifier();
            if (clusterResolver != null) {
                aggregateCluster = clusterResolver.resolveClusterForAggregate(type, aggregateIdentifier);
            }

            createEventClass(aggregateCluster);
            addClusterToTheClassDefinition(aggregateCluster);

            ODocument eventDocument = new ODocument(database, DOMAIN_EVENT_CLASS);
            eventDocument.field("aggregateIdentifier", aggregateIdentifier.asString());
            eventDocument.field("sequenceNumber", event.getSequenceNumber());
            eventDocument.field("timestamp", event.getTimestamp().toString());
            eventDocument.field("body", eventSerializer.serialize(event));
            eventDocument.field("aggregateType", type);

            eventDocument.save(aggregateCluster);
            if (aggregateCluster != null) {
                logger.debug("Event with type \"{}\" id [{}] and sequence number {} was saved to the cluster \"{}\".",
                        new Object[]{type, aggregateIdentifier.asString(), event.getSequenceNumber(),
                                aggregateCluster});
            } else {
                logger.debug("Event with type \"{}\" id [{}] and sequence number {} was saved to the default cluster.",
                        new Object[]{type, event.getSequenceNumber(), aggregateIdentifier.asString()});
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public DomainEventStream readEvents(String type, AggregateIdentifier aggregateIdentifier) {
        String aggregateCluster = null;
        if (clusterResolver != null) {
            aggregateCluster = clusterResolver.resolveClusterForAggregate(type, aggregateIdentifier);
        }

        final String query;
        if (aggregateCluster != null) {
            query = "select * from cluster:" + aggregateCluster +
                    " where aggregateIdentifier = '" + aggregateIdentifier.asString() + "'" +
                    " and aggregateType = '" + type + "'" +
                    " and @class = '" + DOMAIN_EVENT_CLASS + "'  order by sequenceNumber";
        } else {
            query = "select * from " + DOMAIN_EVENT_CLASS +
                    " where aggregateIdentifier = '" + aggregateIdentifier.asString() + "'" +
                    " and aggregateType = '" + type + "'" +
                    " order by sequenceNumber";
        }

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

    @Override
    public void appendSnapshotEvent(String type, DomainEvent snapshotEvent) {
    }


    /**
     * Set OrientDB document oriented database instance that will be used to store DomainEvents.
     *
     * @param database  OrientDB document oriented database instance.
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

    private void createEventClass(String clusterName) {
        OSchema schema = database.getMetadata().getSchema();
        OClass eventClass = schema.getClass(DOMAIN_EVENT_CLASS);

        if (eventClass != null) {
            return;
        }

        if (clusterName != null) {
            eventClass = schema.createClass(DOMAIN_EVENT_CLASS, database.getClusterIdByName(clusterName));
            logger.debug("OClass \"{}\" was created and associated with cluster \"{}\".", DOMAIN_EVENT_CLASS,
                    clusterName);
        } else {
            eventClass = schema.createClass(DOMAIN_EVENT_CLASS);
            logger.debug("OClass \"{}\" was created.", DOMAIN_EVENT_CLASS);
        }

        eventClass.createProperty("aggregateIdentifier", OType.STRING).setMandatory(true).setNotNull(true);
        eventClass.createProperty("sequenceNumber", OType.LONG).setMandatory(true).setNotNull(true);
        eventClass.createProperty("timestamp", OType.STRING).setMin("29").setMax("29").setMandatory(true).
                setNotNull(true);
        eventClass.createProperty("body", OType.BINARY).setMandatory(true).setNotNull(true);
        eventClass.createProperty("aggregateType", OType.STRING).setMandatory(true).setNotNull(true);
    }

    private void addClusterToTheClassDefinition(String clusterName) {
        if (clusterName == null) {
            return;
        }

        final OClass eventClass = database.getMetadata().getSchema().getClass(DOMAIN_EVENT_CLASS);
        final int clusterId = database.getClusterIdByName(clusterName);

        if (Ints.contains(eventClass.getClusterIds(), clusterId)) {
            return;
        }

        eventClass.addClusterIds(clusterId);
        int[] clusterIds = eventClass.getClusterIds();
        clusterIds[clusterIds.length - 1] = clusterId;

        logger.debug("Cluster with name \"{}\" and id [{}] was added to the OClass \"{}\" definition.",
                new Object[]{clusterName, clusterId, DOMAIN_EVENT_CLASS});

    }
}
