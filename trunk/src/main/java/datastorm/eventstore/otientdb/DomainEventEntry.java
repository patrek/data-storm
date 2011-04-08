package datastorm.eventstore.otientdb;

import com.google.common.primitives.Ints;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;
import org.axonframework.eventstore.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
class DomainEventEntry {
    private static final Logger logger = LoggerFactory.getLogger(DomainEventEntry.class);

    /**
     * Name of the document class that will be used to store DomainEvents.
     */
    static final String DOMAIN_EVENT_CLASS = "DomainEvent";

    static final String AGGREGATE_IDENTIFIER_FIELD = "aggregateIdentifier";

    static final String AGGREGATE_TYPE_FIELD = "aggregateType";

    static final String SEQUENCE_NUMBER_FIELD = "sequenceNumber";

    static final String TIMESTAMP_FIELD = "timestamp";

    static final String BODY_FIELD = "body";

    private final EventSerializer eventSerializer;
    private final DomainEvent event;
    private AggregateIdentifier aggregateIdentifier;
    private String aggregateType;

    DomainEventEntry(String aggregateType, DomainEvent event, EventSerializer eventSerializer) {
        this.aggregateType = aggregateType;
        this.aggregateIdentifier = event.getAggregateIdentifier();
        this.event = event;
        this.eventSerializer = eventSerializer;
    }

    DomainEvent getEvent() {
        return event;
    }

    String getAggregateType() {
        return aggregateType;
    }

    ODocument asDocument(ODatabaseDocument databaseDocument, String clusterName) {
        final OClass eventClass = createClass(databaseDocument, clusterName);
        addClusterToTheClassDefinition(databaseDocument, DOMAIN_EVENT_CLASS, clusterName);

        final ODocument eventDocument = new ODocument(eventClass);
        eventDocument.field(AGGREGATE_IDENTIFIER_FIELD, aggregateIdentifier.asString());
        eventDocument.field(SEQUENCE_NUMBER_FIELD, event.getSequenceNumber());
        eventDocument.field(TIMESTAMP_FIELD, event.getTimestamp().toString());
        eventDocument.field(BODY_FIELD, eventSerializer.serialize(event));
        eventDocument.field(AGGREGATE_TYPE_FIELD, aggregateType);

        return eventDocument;
    }

    protected OClass createClass(ODatabaseDocument databaseDocument, String clusterName) {
        final OSchema schema = databaseDocument.getMetadata().getSchema();
        OClass eventClass = schema.getClass(DOMAIN_EVENT_CLASS);

        if (eventClass != null) {
            return eventClass;
        }

        if (clusterName != null) {
            eventClass = schema.createClass(DOMAIN_EVENT_CLASS, databaseDocument.getClusterIdByName(clusterName));
            logger.debug("OClass \"{}\" was created and associated with cluster \"{}\".", DOMAIN_EVENT_CLASS,
                    clusterName);
        } else {
            eventClass = schema.createClass(DOMAIN_EVENT_CLASS);
            logger.debug("OClass \"{}\" was created.", DOMAIN_EVENT_CLASS);
        }

        eventClass.createProperty(AGGREGATE_IDENTIFIER_FIELD, OType.STRING).setMandatory(true).setNotNull(true);
        eventClass.createProperty(SEQUENCE_NUMBER_FIELD, OType.LONG).setMandatory(true).setNotNull(true);
        eventClass.createProperty(TIMESTAMP_FIELD, OType.STRING).setMin("29").setMax("29").setMandatory(true).
                setNotNull(true);
        eventClass.createProperty(BODY_FIELD, OType.BINARY).setMandatory(true).setNotNull(true);
        eventClass.createProperty(AGGREGATE_TYPE_FIELD, OType.STRING).setMandatory(true).setNotNull(true);

        schema.save();

        return eventClass;
    }

    private void addClusterToTheClassDefinition(ODatabaseDocument databaseDocument, String className,
                                                  String clusterName) {
        if (clusterName == null) {
            return;
        }

        final OClass eventClass = databaseDocument.getMetadata().getSchema().getClass(className);
        final int clusterId = databaseDocument.getClusterIdByName(clusterName);

        if (Ints.contains(eventClass.getClusterIds(), clusterId)) {
            return;
        }

        eventClass.addClusterIds(clusterId);
        logger.debug("Cluster with name \"{}\" and id [{}] was added to the OClass \"{}\" definition.",
                new Object[]{clusterName, clusterId, className});

    }
}
