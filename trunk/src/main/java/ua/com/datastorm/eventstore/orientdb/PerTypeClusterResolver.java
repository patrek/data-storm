package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.axonframework.domain.AggregateIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates physical cluster for each new type of the passed in Aggregate.
 *
 * @author Andrey Lomakin
 */
public class PerTypeClusterResolver implements ClusterResolver {

    private static final Logger logger = LoggerFactory.getLogger(PerTypeClusterResolver.class);

    private ODatabaseDocument database;

    /**
     * {@inheritDoc}
     */
    @Override
    public String resolveClusterForAggregate(String type, AggregateIdentifier aggregateIdentifier) {
        final int clusterId = database.getClusterIdByName(type);
        if (clusterId == -1) {
            final int createdClusterId = database.addPhysicalCluster(type, type, -1);
            logger.debug("Cluster with name \"{}\" and id [{}] was created for Aggregate with type \"{}\".",
                    new Object[]{type, createdClusterId, type});
        }

        return type;
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
