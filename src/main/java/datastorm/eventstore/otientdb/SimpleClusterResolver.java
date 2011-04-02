package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.axonframework.domain.AggregateIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the same cluster for Aggregate.
 * Cluster name is passed in as constructor parameter.
 *
 * @author Andrey Lomakin
 */
public class SimpleClusterResolver implements ClusterResolver {
    private static final Logger logger = LoggerFactory.getLogger(SimpleClusterResolver.class);

    private String clusterName;

    private ODatabaseDocument database;

    /**
     * @param clusterName Cluster name that is intended to be created.
     */
    public SimpleClusterResolver(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String resolveClusterForAggregate(String type, AggregateIdentifier aggregateIdentifier) {
        final int clusterId = database.getClusterIdByName(clusterName);
        if (clusterId == -1) {
            final int createdClusterId = database.addPhysicalCluster(clusterName, clusterName, -1);
            logger.debug("Cluster with name \"{}\" and id [{}] was created for Aggregate with type \"{}\".",
                    new Object[]{type, createdClusterId, clusterName});
        }
        return clusterName;
    }

    /**
     * Sets Document Database instance.
     *
     * @param database Document Database instance.
     */
    public void setDatabase(ODatabaseDocument database) {
        this.database = database;
    }
}
