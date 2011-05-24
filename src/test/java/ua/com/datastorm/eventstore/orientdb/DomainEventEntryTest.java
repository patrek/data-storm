package ua.com.datastorm.eventstore.orientdb;

import com.google.common.primitives.Ints;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.axonframework.eventstore.EventSerializer;
import org.axonframework.eventstore.XStreamEventSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.agId;
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertDomainEventSchema;
import static org.junit.Assert.*;

/**
 *  Integration test for {@link DomainEventEntry} class.
 *
 *  @author Andrey Lomakin
 */
public class DomainEventEntryTest {
    private ODatabaseDocumentTx database;
    private EventSerializer eventSerializer = new XStreamEventSerializer();

    @Before
    public void setUp() throws Exception {
        database = new ODatabaseDocumentTx("local:target/default");
        database.create();
    }

    @After
    public void tearDown() throws Exception {
        database.delete();
    }

    @Test
    public void testDocumentAndClassCreated() {
        final SimpleDomainEvent domainEvent = new SimpleDomainEvent(1, agId("1"), "val");
        final DomainEventEntry domainEventEntry = new DomainEventEntry("Simple",
                domainEvent, eventSerializer);

        final ODocument result = domainEventEntry.asDocument(database);

        assertNotNull(result);

        assertDocumentStructure(domainEvent, result);

        final OClass eventClass = result.getSchemaClass();
        assertDomainEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);
    }


    @Test
    public void testDocumentAndClassCreationClassExist() {
        final SimpleDomainEvent domainEvent = new SimpleDomainEvent(1, agId("1"), "val");
        final DomainEventEntry domainEventEntry = new DomainEventEntry("Simple",
                domainEvent, eventSerializer);

        domainEventEntry.asDocument(database);
        final ODocument result = domainEventEntry.asDocument(database);

        assertNotNull(result);

        assertDocumentStructure(domainEvent, result);

        final OClass eventClass = result.getSchemaClass();
        assertDomainEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);
    }


    @Test
    public void testGetters() {
        final SimpleDomainEvent domainEvent = new SimpleDomainEvent(1, agId("1"), "val");
        final DomainEventEntry domainEventEntry = new DomainEventEntry("Simple",
                domainEvent, eventSerializer);

        assertSame(domainEvent, domainEventEntry.getEvent());
        assertEquals("Simple", domainEventEntry.getAggregateType());
    }

    private void assertDocumentStructure(SimpleDomainEvent domainEvent, ODocument result) {
        final Set<String> expectedFieldNames = new HashSet<String>(Arrays.asList(
                DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD,
                DomainEventEntry.SEQUENCE_NUMBER_FIELD,
                DomainEventEntry.AGGREGATE_TYPE_FIELD,
                DomainEventEntry.BODY_FIELD,
                DomainEventEntry.TIMESTAMP_FIELD
        ));

        final Set<String> fieldNames = result.fieldNames();
        assertEquals(expectedFieldNames, fieldNames);

        final Map<String, Object> expectedFieldValues = new HashMap<String, Object>();
        expectedFieldValues.put(DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD, "1");
        expectedFieldValues.put(DomainEventEntry.SEQUENCE_NUMBER_FIELD, 1L);
        expectedFieldValues.put(DomainEventEntry.AGGREGATE_TYPE_FIELD, "Simple");
        expectedFieldValues.put(DomainEventEntry.TIMESTAMP_FIELD, domainEvent.getTimestamp().toString());
        expectedFieldValues.put(DomainEventEntry.BODY_FIELD, eventSerializer.serialize(domainEvent));

        for (String fieldName : fieldNames) {
            final Object fieldValue = result.field(fieldName);
            if (!fieldName.equals(DomainEventEntry.BODY_FIELD)) {
                assertEquals(expectedFieldValues.get(fieldName), fieldValue);
            } else {
                assertArrayEquals((byte[]) expectedFieldValues.get(fieldName), (byte[]) fieldValue);
            }
        }
    }
}
