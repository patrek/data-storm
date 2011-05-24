package ua.com.datastorm.eventstore.orientdb;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
import java.util.Set;

/**
 * An {@link EventStore} implementation that uses Document oriented OrientDB to store DomainEvents in a database.
 * The actual DomainEvent is stored as byte array document field.
 * Other fields are used to store meta-data that allow quick finding of DomainEvents for a
 * specific aggregate in the correct order.
 * <p/>
 * The serializer is used to serialize the events is configurable.
 * By default, the {@link XStreamEventSerializer} is used.
 * <p/>
 * If you would like to decrease space that will be consumed for events and in some way improve performance you can
 * set uo flag {#setLeaveLastSnapshotOnly} to true. This flag forces removing of old snapshot events when new one is
 * added.
 *
 * @author EniSh
 */
public class OrientEventStore implements SnapshotEventStore {
    private static final Logger logger = LoggerFactory.getLogger(OrientEventStore.class);
    private final EventSerializer eventSerializer;

    private ODatabaseDocument database;
    private boolean leaveLastSnapshotOnly = true;
    private boolean checkDomainEventUniqueness = false;


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
        registerEvenUniquenessHook();

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
            logger.debug("Domain event class does not exist, returning empty event stream.");
            return new SimpleDomainEventStream();
        }


        final ODocument snapshotEvent = loadLastSnapshotEvent(type, aggregateIdentifier);

        String query = "select * from " + DomainEventEntry.DOMAIN_EVENT_CLASS +
                " where " + DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                " and " + DomainEventEntry.AGGREGATE_TYPE_FIELD + " = '" + type + "'";

        if (snapshotEvent != null) {
            final long snapshotSequenceNumber = snapshotEvent.<Long>field(DomainEventEntry.SEQUENCE_NUMBER_FIELD);
            query += " and ( " + DomainEventEntry.SEQUENCE_NUMBER_FIELD + " >= " + (snapshotSequenceNumber + 1) + " )";
        }

        query += " order by " + DomainEventEntry.SEQUENCE_NUMBER_FIELD;

        final List<ODocument> queryResult =
                database.query(new OSQLSynchQuery<ODocument>(query));

        if (snapshotEvent != null) {
            queryResult.add(0, snapshotEvent);
        }

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
        registerEvenUniquenessHook();

        if (leaveLastSnapshotOnly) {
            dropSnapshots(type, snapshotEvent.getAggregateIdentifier());
        }

        final SnapshotEventEntry domainEventEntry = new SnapshotEventEntry(type, snapshotEvent, eventSerializer);
        storeEventEntry(domainEventEntry);
    }

    /**
     * Sets whether old snapshots should be removed when new one is appended.
     * This option will improve disk size consumption and overall performance by decreasing of items that are needed
     * to be processed.
     *
     * @param leaveLastSnapshotOnly Flag value.
     */
    public void setLeaveLastSnapshotOnly(boolean leaveLastSnapshotOnly) {
        this.leaveLastSnapshotOnly = leaveLastSnapshotOnly;
    }

    /**
     * Indicates whether old snapshots should be removed when new one is appended.
     *
     * @return Flag value.
     */
    public boolean isLeaveLastSnapshotOnly() {
        return leaveLastSnapshotOnly;
    }

    /**
     * Set OrientDB document oriented database instance that will be used to store DomainEvents.
     *
     * @param database OrientDB document oriented database instance.
     */
    public void setDatabase(ODatabaseDocument database) {
        this.database = database;
    }

    public void setCheckDomainEventUniqueness(boolean checkDomainEventUniqueness) {
        this.checkDomainEventUniqueness = checkDomainEventUniqueness;
    }

    private void dropSnapshots(String aggregateType, AggregateIdentifier aggregateIdentifier) {
        if (!database.getMetadata().getSchema().existsClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS)) {
            logger.debug("Snapshot event class does not exist, nothing will be removed, just exit.");
            return;
        }
        final String command = "delete from " + SnapshotEventEntry.SNAPSHOT_EVENT_CLASS +
                " where " + SnapshotEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                " and " + SnapshotEventEntry.AGGREGATE_TYPE_FIELD + " = '" + aggregateType + "'";

        final int removedSnapshots = database.command(new OCommandSQL(command)).<Number>execute().intValue();
        logger.debug("Command \"{}\" was performed and {} snapshot events were removed.", command, removedSnapshots);
    }

    private ODocument loadLastSnapshotEvent(String aggregateType, AggregateIdentifier aggregateIdentifier) {
        if (!database.getMetadata().getSchema().existsClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS)) {
            logger.debug("Snapshot event class does not exist, nothing will be returned, just exit.");
            return null;
        }

        final String query = "select * from " + SnapshotEventEntry.SNAPSHOT_EVENT_CLASS +
                " where " + SnapshotEventEntry.AGGREGATE_IDENTIFIER_FIELD + " = '" + aggregateIdentifier.asString() + "'" +
                " and " + SnapshotEventEntry.AGGREGATE_TYPE_FIELD + " = '" + aggregateType + "'" +
                " order by " + SnapshotEventEntry.SEQUENCE_NUMBER_FIELD + " desc limit 1";

        final List<ODocument> queryResult =
                database.query(new OSQLSynchQuery<ODocument>(query));

        logger.debug("Query \"{}\" was performed and {} snapshot events were fetched.", query, queryResult.size());

        if (queryResult.isEmpty()) {
            return null;
        }
        return queryResult.get(0);
    }

    private void storeEventEntry(DomainEventEntry domainEventEntry) {
        final DomainEvent event = domainEventEntry.getEvent();
        final String aggregateType = domainEventEntry.getAggregateType();

        final ODocument eventDocument = domainEventEntry.asDocument(database);

        eventDocument.save();
        final OSchema schema = database.getMetadata().getSchema();
        schema.save();

        logger.debug("Event for aggregate type \"{}\", id [{}] and sequence number {} was saved to the default cluster.",
                new Object[]{aggregateType, event.getSequenceNumber(), event.getAggregateIdentifier().asString()});
    }

    private void registerEvenUniquenessHook() {
        if (checkDomainEventUniqueness) {
            logger.debug("DomainEvent uniqueness check is switched on. Trying to register {}.",
                    DomainEventUniquenessHook.class.getName());
            final DomainEventUniquenessHook uniquenessHook = new DomainEventUniquenessHook(database);
            final Set<ORecordHook> hooks = database.getHooks();
            if (!hooks.contains(uniquenessHook)) {
                database.registerHook(uniquenessHook);
                logger.debug("{}  has been registered.", DomainEventUniquenessHook.class.getName());
            } else if (logger.isDebugEnabled()) {
                logger.debug("{} had been already registered.", DomainEventUniquenessHook.class.getName());
            }
        }
    }
}
