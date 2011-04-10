package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.axonframework.domain.DomainEvent;
import org.axonframework.eventstore.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SnapshotEventEntry extends DomainEventEntry {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotEventEntry.class);

    /**
     * Name of the document class that will be used to store snapshot events.
     */
    static final String SNAPSHOT_EVENT_CLASS = "AggregateSnapshot";

    SnapshotEventEntry(String aggregateType, DomainEvent event, EventSerializer eventSerializer) {
        super(aggregateType, event, eventSerializer);
    }

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
