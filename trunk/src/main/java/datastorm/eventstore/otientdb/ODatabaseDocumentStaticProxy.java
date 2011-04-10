package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.cache.ODatabaseRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.util.*;

/**
 * Proxy for {@link ODatabaseDocument}. Delegates all method invocation
 * to database provided by {@link ThreadedODatabaseDocumentFactory}.
 * Implemented without reflection.
 *
 * @author EniSh
 *         Date: 02.04.11
 */
public class ODatabaseDocumentStaticProxy implements ODatabaseDocument {
    private ThreadedODatabaseDocumentFactory databaseFactory;

    /**
     * @param databaseFactory factory which provide database connection
     */
    public ODatabaseDocumentStaticProxy(ThreadedODatabaseDocumentFactory databaseFactory) {
        this.databaseFactory = databaseFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
        return databaseFactory.getThreadLocalDatabase().browseClass(iClassName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(String iClusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iDocument, String iFetchPlan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ORecordInternal<?>> getRecordType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetainRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseRecord setRetainRecords(boolean iValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabaseRecord> DB checkSecurity(String iResource, int iOperation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabaseRecord> DB checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <RET> RET newInstance(String iClassName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countClass(String iClassName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <RET> RET newInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OUser getUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reload(ORecordInternal<?> iObject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reload(ORecordInternal<?> iObject, String iFetchPlan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> save(ORecordInternal<?> iObject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> save(ORecordInternal<?> iObject, String iClusterName) {
        return databaseFactory.getThreadLocalDatabase().save(iObject, iClusterName);
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> delete(ORecordInternal<?> iObject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OTransaction getTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin() {
        return databaseFactory.getThreadLocalDatabase().begin();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin(OTransaction.TXTYPE iStatus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin(OTransaction iTx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> commit() {
        return databaseFactory.getThreadLocalDatabase().commit();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> rollback() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RET extends List<?>> RET query(OQuery<? extends Object> iCommand, Object... iArgs) {
        return (RET) databaseFactory.getThreadLocalDatabase().query(iCommand, iArgs);
    }

    @Override
    public <RET extends OCommandRequest> RET command(OCommandRequest iCommand) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OMetadata getMetadata() {
        return databaseFactory.getThreadLocalDatabase().getMetadata();
    }

    @Override
    public ODictionary<ORecordInternal<?>> getDictionary() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<?> getDatabaseOwner() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseComplex<?> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabase> DB getUnderlying() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabaseComplex<?>> DB registerHook(ORecordHook iHookImpl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ORecordHook> getHooks() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabaseComplex<?>> DB unregisterHook(ORecordHook iHookImpl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean callbackHooks(ORecordHook.TYPE iType, OIdentifiable iObject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabase> DB open(String iUserName, String iUserPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <DB extends ODatabase> DB create() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void declareIntent(OIntent iIntent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getURL() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OStorage getStorage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ODatabaseRecordCache getCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUseCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUseCache(boolean useCache) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDefaultClusterId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> getClusterNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClusterIdByName(String iClusterName) {
        return databaseFactory.getThreadLocalDatabase().getClusterIdByName(iClusterName);
    }

    @Override
    public String getClusterType(String iClusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClusterNameById(int iClusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countClusterElements(int iCurrentClusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countClusterElements(int[] iClusterIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countClusterElements(String iClusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int addLogicalCluster(String iClusterName, int iPhyClusterContainerId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int addPhysicalCluster(String iClusterName, String iClusterFileName, int iStartSize) {
        return databaseFactory.getThreadLocalDatabase().addPhysicalCluster(iClusterName, iClusterFileName, iStartSize);
    }

    @Override
    public int addDataSegment(String iSegmentName, String iSegmentFileName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object setProperty(String iName, Object iValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty(String iName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<String, Object>> getProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerListener(ODatabaseListener iListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterListener(ODatabaseListener iListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getUserObjectByRecord(ORecordInternal<?> iRecord, String iFetchPlan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean existsUserObjectByRID(ORID iRID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerPojo(Object iObject, ORecordInternal<?> iRecord) {
        throw new UnsupportedOperationException();
    }
}
