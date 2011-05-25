package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates new connections to Orient database. It use global connection pull for establishing connection.
 *
 * @author EniSh
 */
public class ConnectionManager {
    private final String databaseURL;
    private final String databaseUserName;
    private final String databasePassword;
    private final ODatabaseDocumentPool globalDatabasePool;
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    /**
     * Simple constructor for connection manager. Create manager which use <code>ODatabaseDocumentPool.global()</code>.
     *
     * @param databaseURL
     * @param databaseUserName
     * @param databasePassword
     */
    public ConnectionManager(final String databaseURL, final String databaseUserName, final String databasePassword) {
        this.globalDatabasePool = ODatabaseDocumentPool.global();
        this.databaseURL = databaseURL;
        this.databaseUserName = databaseUserName;
        this.databasePassword = databasePassword;
    }

    /**
     * Create manager which use specific connection pull.
     *
     * @param databaseURL
     * @param databaseUserName
     * @param databasePassword
     * @param pool             - connection pull
     */
    public ConnectionManager(final String databaseURL, final String databaseUserName, final String databasePassword,
                             ODatabaseDocumentPool pool) {
        this.globalDatabasePool = pool;
        this.databaseURL = databaseURL;
        this.databaseUserName = databaseUserName;
        this.databasePassword = databasePassword;
    }

    /**
     * Acquire new database connection from database pool.
     *
     * @return new database connection
     */
    public ODatabaseDocument getNewConnection() {
        logger.debug("Creating new connection to {} [username = {}, password = {}]", new Object[]{databaseURL, databaseUserName, databasePassword});
        return globalDatabasePool.acquire(databaseURL, databaseUserName, databasePassword);
    }
}
