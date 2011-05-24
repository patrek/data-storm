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
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertSnapshotEventSchema;
import static org.junit.Assert.*;

/**
 *  Integration test for {@link SnapshotEventEntry} class.
 *
 *  @author Andrey Lomakin
 */
public class SnapshotEventEntryTest {
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
        final SnapshotEventEntry snapshotEventEntry = new SnapshotEventEntry("Simple",
                domainEvent, eventSerializer);


        final ODocument result = snapshotEventEntry.asDocument(database);

        assertNotNull(result);

        assertDocumentStructure(domainEvent, result);

        final OClass eventClass = result.getSchemaClass();
        assertSnapshotEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);

    }

    @Test
    public void testDocumentAndClassCreationClassExist() {
        final SimpleDomainEvent domainEvent = new SimpleDomainEvent(1, agId("1"), "val");
        final SnapshotEventEntry snapshotEventEntry = new SnapshotEventEntry("Simple",
                domainEvent, eventSerializer);

        snapshotEventEntry.asDocument(database);
        final ODocument result = snapshotEventEntry.asDocument(database);

        assertNotNull(result);

        assertDocumentStructure(domainEvent, result);

        final OClass eventClass = result.getSchemaClass();
        assertSnapshotEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);
    }


    private void assertDocumentStructure(SimpleDomainEvent domainEvent, ODocument result) {
        final Set<String> expectedFieldNames = new HashSet<String>(Arrays.asList(
                SnapshotEventEntry.AGGREGATE_IDENTIFIER_FIELD,
                SnapshotEventEntry.SEQUENCE_NUMBER_FIELD,
                SnapshotEventEntry.AGGREGATE_TYPE_FIELD,
                SnapshotEventEntry.BODY_FIELD,
                SnapshotEventEntry.TIMESTAMP_FIELD
        ));

        final Set<String> fieldNames = result.fieldNames();
        assertEquals(expectedFieldNames, fieldNames);

        final Map<String, Object> expectedFieldValues = new HashMap<String, Object>();
        expectedFieldValues.put(SnapshotEventEntry.AGGREGATE_IDENTIFIER_FIELD, "1");
        expectedFieldValues.put(SnapshotEventEntry.SEQUENCE_NUMBER_FIELD, 1L);
        expectedFieldValues.put(SnapshotEventEntry.AGGREGATE_TYPE_FIELD, "Simple");
        expectedFieldValues.put(SnapshotEventEntry.TIMESTAMP_FIELD, domainEvent.getTimestamp().toString());
        expectedFieldValues.put(SnapshotEventEntry.BODY_FIELD, eventSerializer.serialize(domainEvent));

        for (String fieldName : fieldNames) {
            final Object fieldValue = result.field(fieldName);
            if (!fieldName.equals(SnapshotEventEntry.BODY_FIELD)) {
                assertEquals(expectedFieldValues.get(fieldName), fieldValue);
            } else {
                assertArrayEquals((byte[]) expectedFieldValues.get(fieldName), (byte[]) fieldValue);
            }
        }
    }
}
