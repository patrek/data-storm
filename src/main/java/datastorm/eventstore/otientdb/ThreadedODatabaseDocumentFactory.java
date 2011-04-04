package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * Creates own database connection for each thread.
 *
 * @author EniSh
 *         Date: 04.04.11
 */
public class ThreadedODatabaseDocumentFactory {
    private ThreadLocal<ODatabaseDocument> database = new ThreadLocal<ODatabaseDocument>();
    private String databaseURL;
    private String databaseUserName;
    private String databasePassword;

    public ThreadedODatabaseDocumentFactory(String databaseURL, String databaseUserName, String databasePassword) {
        this.databaseURL = databaseURL;
        this.databaseUserName = databaseUserName;
        this.databasePassword = databasePassword;
    }

    public ODatabaseDocument getThreadLocalDatabase() {
        ODatabaseDocument databaseDocument = database.get();
        if (databaseDocument == null) {
            databaseDocument = new ODatabaseDocumentTx(databaseURL).open(databaseUserName, databasePassword);
// Works fine while using less then 50 connections, but fails
//            databaseDocument = new ODatabaseDocumentPool().global().acquire(databaseURL, databaseUserName, databasePassword);
            database.set(databaseDocument);
        }
        return databaseDocument;
    }
}
