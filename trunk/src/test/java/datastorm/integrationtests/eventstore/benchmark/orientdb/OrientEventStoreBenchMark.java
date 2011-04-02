package datastorm.integrationtests.eventstore.benchmark.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import datastorm.eventstore.otientdb.OrientEventStore;
import datastorm.integrationtests.eventstore.benchmark.AbstractEventStoreBenchmark;
import org.axonframework.domain.UUIDAggregateIdentifier;

/**
 * @author EniSh
 *         Date: 30.03.11
 */
public class OrientEventStoreBenchMark extends AbstractEventStoreBenchmark {
    private OrientEventStore eventStore;
    private ODatabaseDocument database;

    public OrientEventStoreBenchMark(OrientEventStore eventStore, ODatabaseDocument database) {
        this.eventStore = eventStore;
        this.database = database;
    }

    public static void main(String[] args) throws Exception {
        AbstractEventStoreBenchmark benchmark = prepareBenchMark("spring/benchmark-orient-context.xml");
        benchmark.startBenchMark();
    }

    @Override
    protected void prepareEventStore() {
    }

    @Override
    protected Runnable getRunnableInstance() {
        return new OrientBenchmark();
    }

    private class OrientBenchmark implements Runnable {

        @Override
        public void run() {
            UUIDAggregateIdentifier aggregateId = new UUIDAggregateIdentifier();
            database.begin();
            int eventSequence = 0;
            for (int t = 0; t < getTransactionCount(); t++) {
                eventSequence = saveAndLoadLargeNumberOfEvents(aggregateId, eventStore, eventSequence);
            }
            database.commit();
        }
    }
}
