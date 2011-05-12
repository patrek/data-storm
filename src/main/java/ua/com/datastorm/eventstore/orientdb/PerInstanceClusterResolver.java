package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.axonframework.domain.AggregateIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates physical cluster for each new instance of the passed in Aggregate.
 *
 * @author Andrey Lomakin
 */
public class PerInstanceClusterResolver implements ClusterResolver {
    private static final Logger logger = LoggerFactory.getLogger(PerInstanceClusterResolver.class);

    private ODatabaseDocument database;

    /**
     * {@inheritDoc}
     */
    public String resolveClusterForAggregate(String type, AggregateIdentifier aggregateIdentifier) {
        final String iClusterName = composeClusterName(type, aggregateIdentifier);
        final int clusterId = database.getClusterIdByName(iClusterName);
        if (clusterId == -1) {
            final int createdClusterId = database.addPhysicalCluster(iClusterName, iClusterName, -1);
            logger.debug("Cluster with name \"{}\" and id [{}] was created for Aggregate with type \"{}\" and id [{}].",
                    new Object[]{iClusterName, createdClusterId, type, aggregateIdentifier.asString()});
        }

        return iClusterName;
    }

    /**
     * Sets Document Database instance.
     *
     * @param database Document Database instance.
     */
    public void setDatabase(ODatabaseDocument database) {
        this.database = database;
    }

    private String composeClusterName(String type, AggregateIdentifier aggregateIdentifier) {
        return type + "." + aggregateIdentifier.asString();
    }
}
