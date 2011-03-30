package datastorm.eventstore.otientdb;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.SimpleDomainEventStream;
import org.axonframework.eventstore.EventSerializer;
import org.axonframework.eventstore.EventStore;
import org.axonframework.eventstore.XStreamEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An {@link EventStore} implementation that uses OrientDB to store DomainEvents in a database.
 *
 * @author EniSh
 */
public class OrientEventStore implements EventStore {
    private static final Logger logger = LoggerFactory.getLogger(OrientEventStore.class);

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
            final DomainEvent event = domainEventStream.next();

            String aggregateCluster = null;

            final AggregateIdentifier aggregateIdentifier = event.getAggregateIdentifier();
            if (clusterResolver != null) {
                aggregateCluster = clusterResolver.resolveClusterForAggregate(type, aggregateIdentifier);
            }

            createEventClass(type, aggregateCluster);
            addClusterToTheClassDefinition(type, aggregateCluster);

            ODocument eventDocument = new ODocument(database, type);
            eventDocument.field("aggregateIdentifier", aggregateIdentifier.asString());
            eventDocument.field("sequenceNumber", event.getSequenceNumber());
            eventDocument.field("timestamp", event.getTimestamp().toString());
            eventDocument.field("body", eventSerializer.serialize(event));

            eventDocument.save(aggregateCluster);
            if(aggregateCluster != null) {
                logger.debug("Aggregate with type \"{}\" and id [{}] was saved to the cluster \"{}\".",
                        new Object[] {type, aggregateIdentifier.asString(), aggregateCluster});
            } else {
                logger.debug("Aggregate with type \"{}\" and id [{}] was saved to the default cluster.",
                        new Object[] {type, aggregateIdentifier.asString()});
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
                    " where aggregateIdentifier = '" + aggregateIdentifier.asString() +
                    "' order by sequenceNumber";
        } else {
            query = "select * from " + type +
                    " where aggregateIdentifier = '" + aggregateIdentifier.asString() +
                    "' order by sequenceNumber";
        }

        final List<ODocument> queryResult =
                database.query(new OSQLSynchQuery<ODocument>(query));

        logger.debug("Query \"{}\" was performed and {} items were fetched.", query, queryResult.size());

        return new SimpleDomainEventStream(Collections2.transform(queryResult,
                new Function<ODocument, DomainEvent>() {
                    @Override
                    public DomainEvent apply(ODocument document) {
                        return eventSerializer.deserialize(document.<byte[]>field("body"));
                    }
                }));
    }

    public void setDatabase(ODatabaseDocumentTx database) {
        this.database = database;
    }

    public void setClusterResolver(ClusterResolver clusterResolver) {
        this.clusterResolver = clusterResolver;
    }

    private void createEventClass(String type, String clusterName) {
        OSchema schema = database.getMetadata().getSchema();
        OClass eventClass = schema.getClass(type);

        if (eventClass != null) {
            return;
        }

        if (clusterName != null) {
            eventClass = schema.createClass(type, database.getClusterIdByName(clusterName));
            logger.debug("OClass \"{}\" was created and associated with cluster \"{}\".", type, clusterName);
        } else {
            eventClass = schema.createClass(type);
            logger.debug("OClass \"{}\" was created.", type, clusterName);
        }

        eventClass.createProperty("aggregateIdentifier", OType.STRING).setMandatory(true).setNotNull(true);
        eventClass.createProperty("sequenceNumber", OType.LONG).setMandatory(true).setNotNull(true);
        eventClass.createProperty("timestamp", OType.STRING).setMin("29").setMax("29").setMandatory(true).
                setNotNull(true);
        eventClass.createProperty("body", OType.BINARY).setMandatory(true).setNotNull(true);
    }

    private void addClusterToTheClassDefinition(String type, String clusterName) {
        if (clusterName == null) {
            return;
        }

        final OClass eventClass = database.getMetadata().getSchema().getClass(type);
        final int clusterId = database.getClusterIdByName(clusterName);

        if (Ints.contains(eventClass.getClusterIds(), clusterId)) {
            return;
        }

        eventClass.addClusterIds(clusterId);
        int[] clusterIds = eventClass.getClusterIds();
        clusterIds[clusterIds.length - 1] = clusterId;

        logger.debug("Cluster with name \"{}\" and id [{}] was added to the OClass \"{}\" definition.",
                new Object[] {clusterName, clusterId, type});

    }
}
