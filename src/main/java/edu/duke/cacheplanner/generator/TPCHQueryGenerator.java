package edu.duke.cacheplanner.generator;

import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.duke.cacheplanner.data.TPCHQueueDistribution;
import edu.duke.cacheplanner.query.TPCHQuery;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.listener.QuerySerialize;
import edu.duke.cacheplanner.query.AbstractQuery;

public class TPCHQueryGenerator extends AbstractQueryGenerator {
  private int count = 0;
  protected double waitingTime;

  public TPCHQueryGenerator(double lamb, int id, String name) {
    super(lamb, id, name);
    waitingTime = 0;
    generatorThread = createThread();
  }

  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public AbstractQuery generateQuery() {
    int queryId = count++;
    TPCHQuery query = new TPCHQuery(queryId, queueId, "", "", 0);

    Random rand = new Random();
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    TPCHQueueDistribution distribution = queryDistribution.getTPCHQueueDistribution(queueId);
    Map<TPCHQuery, Double> distributionMap = distribution.getQueueDistributionMap();
    for(TPCHQuery q : distributionMap.keySet()) {
      cumulativeProb = cumulativeProb + distributionMap.get(q);
      if(p <= cumulativeProb) {
        query.setPath(q.getPath());
	query.setCachedPath(q.getCachedPath());
	query.setBenefit(q.getBenefit());
	break;
      }
    }

    query.setWeight(externalQueue.getWeight());
    return query;
  }

  @Override
  public Thread createThread() {
    return new Thread("QueryGenerator") {
        @Override
        public void run() {
          while (true) {
            if (!started) {
              return;
            }
            //get delay
            long delay = (long) getPoissonDelay();
            waitingTime = delay;

            try {
              Thread.sleep(delay);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            //generate the query & post the event to the listener
            AbstractQuery query = generateQuery();
            query.setTimeDelay(waitingTime);
            externalQueue.addQuery(query);
            listenerManager.postEvent(new QuerySerialize(query));
            listenerManager.postEvent(new QueryGenerated(query));
          }
        }
      };
  }

}
