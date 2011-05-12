package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;

/**
 * @author EniSh
 *         Date: 23.04.11
 */
public class ConnectionManager {
    private final String databaseURL;
    private final String databaseUserName;
    private final String databasePassword;
    private final ODatabaseDocumentPool globalDatabasePool = ODatabaseDocumentPool.global();

    public ConnectionManager(final String databaseURL, final String databaseUserName, final String databasePassword) {
        this.databaseURL = databaseURL;
        this.databaseUserName = databaseUserName;
        this.databasePassword = databasePassword;
    }

    public ConnectionManager(final String databaseURL, final String databaseUserName, final String databasePassword,
                             final int minPoolSize, final int maxPoolSize) {
        this.databaseURL = databaseURL;
        this.databaseUserName = databaseUserName;
        this.databasePassword = databasePassword;
        globalDatabasePool.setup(minPoolSize, maxPoolSize);
    }

    public ODatabaseDocument getNewConnection() {
        return globalDatabasePool.acquire(databaseURL, databaseUserName, databasePassword);
    }
}
