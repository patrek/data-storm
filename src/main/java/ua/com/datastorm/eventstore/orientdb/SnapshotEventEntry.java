package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.axonframework.domain.DomainEvent;
import org.axonframework.eventstore.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Presentation of OrientDb document that will contain Snapshot Event data and also metadata that will
 * be used in queries to find given Snapshot Event.
 * <p/>
 * Instance of given document can be created by calling of {@link #asDocument(ODatabaseDocument, String)} method.
 * <p/>
 * Document will have class named {@link #SNAPSHOT_EVENT_CLASS}.
 * Given class does not have its own fields and is derived form {@link #DOMAIN_EVENT_CLASS}.
 *
 * @see DomainEventEntry
 *
 * @author Andrey Lomakin
 */
class SnapshotEventEntry extends DomainEventEntry {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotEventEntry.class);

    /**
     * Name of the document class that will be used to store snapshot events.
     */
    static final String SNAPSHOT_EVENT_CLASS = "AggregateSnapshot";

    /**
     * {@inheritDoc}
     */
    SnapshotEventEntry(String aggregateType, DomainEvent event, EventSerializer eventSerializer) {
        super(aggregateType, event, eventSerializer);
    }

    /**
     * Creates document class that presents Snapshot Event data and metadata.
     * <p/>
     * Class does not have its own fields and simple extends {@link #DOMAIN_EVENT_CLASS}.
     *
     * @param databaseDocument Current database instance.
     * @param clusterName      Cluster name where document is going to be stored. Can be null.
     *                         In last case default cluster should be used.
     * @return Document class that presents Snapshot Event and auxiliary metadata.
     */
    @Override
    protected OClass createClass(ODatabaseDocument databaseDocument, String clusterName) {
        final OSchema schema = databaseDocument.getMetadata().getSchema();
        OClass eventClass = schema.getClass(SNAPSHOT_EVENT_CLASS);

        if (eventClass != null) {
            return eventClass;
        }

        final int clusterId;
        if (clusterName != null) {
            clusterId = databaseDocument.getClusterIdByName(clusterName);
            logger.debug("OClass \"{}\" was created and associated with cluster \"{}\".", SNAPSHOT_EVENT_CLASS,
                    clusterName);
        } else {
            clusterId = databaseDocument.getDefaultClusterId();
            logger.debug("OClass \"{}\" was created.", SNAPSHOT_EVENT_CLASS);
        }

        eventClass = schema.createClass(SNAPSHOT_EVENT_CLASS, clusterId);
        final OClass parentClass = super.createClass(databaseDocument, clusterName);
        eventClass.setSuperClass(parentClass);
        return eventClass;
    }
}
