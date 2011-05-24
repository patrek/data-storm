package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.index.OIndexCallback;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;


class DomainEventUniquenessHook extends ODocumentHookAbstract implements OIndexCallback {
    static final String EVENT_UNIQUENESS_INDEX_NAME = DomainEventUniquenessHook.class.getName() +
            ".EVENT_UNIQUENESS_INDEX";

    private OIndexUnique uniquenessIndex;
    private ODatabaseDocument database;

    DomainEventUniquenessHook(ODatabaseDocument database) {
        this.database = database;

        final OIndexManager indexManager = database.getMetadata().getIndexManager();
        uniquenessIndex = (OIndexUnique) indexManager.getIndex(EVENT_UNIQUENESS_INDEX_NAME);
    }


    @Override
    public boolean onRecordBeforeCreate(ODocument iDocument) {
        if (isDomainEvent(iDocument)) {
            setupIndex(iDocument);
            uniquenessIndex.checkEntry(iDocument, generateKey(iDocument));
        }
        return false;
    }

    @Override
    public boolean onRecordAfterCreate(ODocument iDocument) {
        if (isDomainEvent(iDocument)) {
            uniquenessIndex.put(generateKey(iDocument), iDocument.placeholder());
            uniquenessIndex.lazySave();
        }
        return false;
    }

    @Override
    public boolean onRecordBeforeUpdate(ODocument iDocument) {
        if (isDomainEvent(iDocument)) {
            throw new IllegalStateException("DomainEvent documents can not be updated.");
        }
        return false;
    }

    @Override
    public boolean onRecordAfterDelete(ODocument iDocument) {
        if (isDomainEvent(iDocument)) {
            setupIndex(iDocument);

            uniquenessIndex.remove(generateKey(iDocument));
            uniquenessIndex.lazySave();
        }
        return false;
    }

      @Override
    public Object getDocumentValueToIndex(ODocument iDocument) {
        return generateKey(iDocument);
    }

    private boolean isDomainEvent(ODocument iDocument) {
        final OClass schemaClass = iDocument.getSchemaClass();
        return schemaClass != null &&
                (schemaClass.getName().equals(DomainEventEntry.DOMAIN_EVENT_CLASS));
    }

    private void setupIndex(ODocument iDocument) {
        final OClass schemaClass = iDocument.getSchemaClass();
        if (uniquenessIndex == null) {
            final OIndexManager indexManager = database.getMetadata().getIndexManager();
            uniquenessIndex = (OIndexUnique)
                    indexManager.createIndex(EVENT_UNIQUENESS_INDEX_NAME, OProperty.INDEX_TYPE.UNIQUE.toString(),
                            schemaClass.getClusterIds(), this, null, true);
            uniquenessIndex.rebuild();
        }
    }

    private Object generateKey(final ODocument iRecord) {
        return iRecord.field(DomainEventEntry.AGGREGATE_IDENTIFIER_FIELD) + "+" +
                iRecord.field(DomainEventEntry.SEQUENCE_NUMBER_FIELD) + "+" +
                iRecord.field(DomainEventEntry.AGGREGATE_TYPE_FIELD);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DomainEventUniquenessHook that = (DomainEventUniquenessHook) o;

        return database.equals(that.database);
    }

    @Override
    public int hashCode() {
        return database.hashCode();
    }

}
