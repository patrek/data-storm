package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.axonframework.domain.AggregateIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates cluster for each new instance of the passed in Aggregate.
 *
 * @author Andrey Lomakin
 */
public class PerInstanceClusterResolver implements ClusterResolver {
    private static final Logger logger = LoggerFactory.getLogger(PerInstanceClusterResolver.class);

    private ODatabaseDocumentTx database;

    /**
     * {@inheritDoc}
     */
    public String resolveClusterForAggregate(String type, AggregateIdentifier aggregateIdentifier) {
        String iClusterName = composeClusterName(type, aggregateIdentifier);
        final int clusterId = database.getClusterIdByName(iClusterName);
        if(clusterId == -1) {
            database.addPhysicalCluster(iClusterName);
            logger.debug("Cluster with name \"{}\" for Aggregate type {} with id:[{}] was created.",
                    new Object[] {iClusterName, type, aggregateIdentifier.asString()});
        }

        return iClusterName;
    }

    /**
     * Sets Document Database instance.
     * @param database Document Database instance.
     */
    public void setDatabase(ODatabaseDocumentTx database) {
        this.database = database;
    }

    private String composeClusterName(String type, AggregateIdentifier aggregateIdentifier) {
        return type + "." + aggregateIdentifier.asString();
    }
}
