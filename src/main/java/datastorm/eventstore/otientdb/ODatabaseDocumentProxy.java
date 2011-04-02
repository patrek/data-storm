package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.cache.ODatabaseRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Thread safe implementation of ODatabaseDocumentTx.
 * Creates own database connection for each thread.
 *
 * @author EniSh
 *         Date: 02.04.11
 */
public class ODatabaseDocumentProxy implements ODatabaseDocument {
    private ThreadLocal<ODatabaseDocument> database = new ThreadLocal<ODatabaseDocument>();
    private String databaseURL;
    private String databaseUserName;
    private String databasePassword;

    private ODatabaseDocument getThreadLocalDatebase() {
        ODatabaseDocument databaseDocument = database.get();
        if (databaseDocument == null) {
            databaseDocument = new ODatabaseDocumentTx(databaseURL).open(databaseUserName, databasePassword);
// Works fine while using less then 50 connections, but fails
//            databaseDocument = new ODatabaseDocumentPool().global().acquire(databaseURL, databaseUserName, databasePassword);
            database.set(databaseDocument);
        }
        return databaseDocument;
    }

    public void setDatabaseURL(String databaseURL) {
        this.databaseURL = databaseURL;
    }

    public void setDatabaseUserName(String databaseUserName) {
        this.databaseUserName = databaseUserName;
    }

    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    @Override
    public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
        return getThreadLocalDatebase().browseClass(iClassName);
    }

    public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
        throw new NotImplementedException();
    }

    public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
        throw new NotImplementedException();
    }

    public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
        throw new NotImplementedException();
    }

    @Override
    public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(String iClusterName) {
        throw new NotImplementedException();
    }

    @Override
    public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass) {
        throw new NotImplementedException();
    }

    @Override
    public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iDocument, String iFetchPlan) {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends ORecordInternal<?>> getRecordType() {
        throw new NotImplementedException();
    }

    @Override
    public boolean isRetainRecords() {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseRecord setRetainRecords(boolean iValue) {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabaseRecord> DB checkSecurity(String iResource, int iOperation) {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabaseRecord> DB checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
        throw new NotImplementedException();
    }

    @Override
    public <RET> RET newInstance(String iClassName) {
        throw new NotImplementedException();
    }

    @Override
    public long countClass(String iClassName) {
        throw new NotImplementedException();
    }

    @Override
    public <RET> RET newInstance() {
        throw new NotImplementedException();
    }

    @Override
    public OUser getUser() {
        throw new NotImplementedException();
    }

    @Override
    public void reload(ORecordInternal<?> iObject) {
        throw new NotImplementedException();
    }

    @Override
    public void reload(ORecordInternal<?> iObject, String iFetchPlan) {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> save(ORecordInternal<?> iObject) {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> save(ORecordInternal<?> iObject, String iClusterName) {
        return getThreadLocalDatebase().save(iObject, iClusterName);
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> delete(ORecordInternal<?> iObject) {
        throw new NotImplementedException();
    }

    @Override
    public OTransaction getTransaction() {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin() {
        return getThreadLocalDatebase().begin();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin(OTransaction.TXTYPE iStatus) {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> begin(OTransaction iTx) {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> commit() {
        return getThreadLocalDatebase().commit();
    }

    @Override
    public ODatabaseComplex<ORecordInternal<?>> rollback() {
        throw new NotImplementedException();
    }

    @Override
    public <RET extends List<?>> RET query(OQuery<? extends Object> iCommand, Object... iArgs) {
        throw new NotImplementedException();
    }

    @Override
    public <RET extends OCommandRequest> RET command(OCommandRequest iCommand) {
        throw new NotImplementedException();
    }

    @Override
    public OMetadata getMetadata() {
        return getThreadLocalDatebase().getMetadata();
    }

    @Override
    public ODictionary<ORecordInternal<?>> getDictionary() {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<?> getDatabaseOwner() {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseComplex<?> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabase> DB getUnderlying() {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabaseComplex<?>> DB registerHook(ORecordHook iHookImpl) {
        throw new NotImplementedException();
    }

    @Override
    public Set<ORecordHook> getHooks() {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabaseComplex<?>> DB unregisterHook(ORecordHook iHookImpl) {
        throw new NotImplementedException();
    }

    @Override
    public boolean callbackHooks(ORecordHook.TYPE iType, OIdentifiable iObject) {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabase> DB open(String iUserName, String iUserPassword) {
        throw new NotImplementedException();
    }

    @Override
    public <DB extends ODatabase> DB create() {
        throw new NotImplementedException();
    }

    @Override
    public void delete() {
        throw new NotImplementedException();
    }

    @Override
    public void declareIntent(OIntent iIntent) {
        throw new NotImplementedException();
    }

    @Override
    public boolean exists() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }

    @Override
    public int getId() {
        throw new NotImplementedException();
    }

    @Override
    public String getName() {
        throw new NotImplementedException();
    }

    @Override
    public String getURL() {
        throw new NotImplementedException();
    }

    @Override
    public OStorage getStorage() {
        throw new NotImplementedException();
    }

    @Override
    public ODatabaseRecordCache getCache() {
        throw new NotImplementedException();
    }

    @Override
    public boolean isUseCache() {
        throw new NotImplementedException();
    }

    @Override
    public void setUseCache(boolean useCache) {
        throw new NotImplementedException();
    }

    @Override
    public int getDefaultClusterId() {
        throw new NotImplementedException();
    }

    @Override
    public Collection<String> getClusterNames() {
        throw new NotImplementedException();
    }

    @Override
    public int getClusterIdByName(String iClusterName) {
        return getThreadLocalDatebase().getClusterIdByName(iClusterName);
    }

    @Override
    public String getClusterType(String iClusterName) {
        throw new NotImplementedException();
    }

    @Override
    public String getClusterNameById(int iClusterId) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isClosed() {
        throw new NotImplementedException();
    }

    @Override
    public long countClusterElements(int iCurrentClusterId) {
        throw new NotImplementedException();
    }

    @Override
    public long countClusterElements(int[] iClusterIds) {
        throw new NotImplementedException();
    }

    @Override
    public long countClusterElements(String iClusterName) {
        throw new NotImplementedException();
    }

    @Override
    public int addLogicalCluster(String iClusterName, int iPhyClusterContainerId) {
        throw new NotImplementedException();
    }

    @Override
    public int addPhysicalCluster(String iClusterName, String iClusterFileName, int iStartSize) {
        return getThreadLocalDatebase().addPhysicalCluster(iClusterName, iClusterFileName, iStartSize);
    }

    @Override
    public int addDataSegment(String iSegmentName, String iSegmentFileName) {
        throw new NotImplementedException();
    }

    @Override
    public Object setProperty(String iName, Object iValue) {
        throw new NotImplementedException();
    }

    @Override
    public Object getProperty(String iName) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Map.Entry<String, Object>> getProperties() {
        throw new NotImplementedException();
    }

    @Override
    public void registerListener(ODatabaseListener iListener) {
        throw new NotImplementedException();
    }

    @Override
    public void unregisterListener(ODatabaseListener iListener) {
        throw new NotImplementedException();
    }

    @Override
    public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
        throw new NotImplementedException();
    }

    @Override
    public Object getUserObjectByRecord(ORecordInternal<?> iRecord, String iFetchPlan) {
        throw new NotImplementedException();
    }

    @Override
    public boolean existsUserObjectByRID(ORID iRID) {
        throw new NotImplementedException();
    }

    @Override
    public void registerPojo(Object iObject, ORecordInternal<?> iRecord) {
        throw new NotImplementedException();
    }
}
