package edu.duke.cacheplanner.generator;

import java.util.Queue;

import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.listener.QuerySerialize;
import edu.duke.cacheplanner.query.AbstractQuery;

public class ReplayQueryGenerator extends AbstractQueryGenerator {
	protected Queue<AbstractQuery> myQueries;
	public ReplayQueryGenerator(double lamb, int id, Queue<AbstractQuery> queries) {
		super(0, id);
		myQueries = queries;
	}
	@Override
	public Thread createThread() {
	    return new Thread("QueryGenerator") {
	        @Override
	        public void run() {
	          boolean status = true;
	          while (status) {
	            if (!started) {
	              return;
	            }
	            AbstractQuery query = generateQuery();
	            if(query == null) {
	            	status = false;
	            }
	            else{
		            long delay = (long) query.getTimeDelay();
		            System.out.println("wait for " + delay);
		            try {
		              Thread.sleep(delay);
		            } catch (InterruptedException e) {
		              e.printStackTrace();
		            }
		            externalQueue.addQuery(query);
		            listenerManager.postEvent(new QuerySerialize(query));
		            listenerManager.postEvent(new QueryGenerated
		                    (Integer.parseInt(query.getQueryID()), Integer.parseInt(query.getQueueID())));
	            }
	          }
	        }
	    };
	}

	@Override
	public AbstractQuery generateQuery() {
		return myQueries.poll();
	}

}
