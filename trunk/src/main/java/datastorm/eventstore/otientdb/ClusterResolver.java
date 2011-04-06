package datastorm.eventstore.otientdb;

import org.axonframework.domain.AggregateIdentifier;

/**
 * OrientDb allows to create clusters (physical or logical).
 * <p/>
 * This strategy composes cluster name and decides whether it is needed
 * to create new cluster (physical or logical) to store events of the given Aggregate.
 *
 * @author Andrey Lomakin
 * @see <a href="http://code.google.com/p/orient/wiki/Concepts#Cluster">Cluster Concept</a>
 */
public interface ClusterResolver {
    /**
     * Creates new cluster for the given Aggregate instance or returns name of existing one.
     *
     * @param type                Aggregate type.
     * @param aggregateIdentifier Aggregate Identifier
     * @return Cluster name for the given aggregate.
     */
    String resolveClusterForAggregate(String type, AggregateIdentifier aggregateIdentifier);
}
