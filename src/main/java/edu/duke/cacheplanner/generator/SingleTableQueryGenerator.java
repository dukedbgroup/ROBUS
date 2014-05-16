package edu.duke.cacheplanner.generator;

import java.util.ArrayList;
import java.util.List;

import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.query.Projection;
import edu.duke.cacheplanner.query.Selection;
import edu.duke.cacheplanner.query.SingleTableQuery;
import edu.duke.cacheplanner.queue.ExternalQueue;

public class SingleTableQueryGenerator extends AbstractQueryGenerator {
	private int count = 0;
	
    public SingleTableQueryGenerator(double lamb, int id, int colNum) {
	    super(lamb, id, colNum);
	}

	public SingleTableQueryGenerator(double lamb, ExternalQueue queue,
			ListenerManager manager, List<Dataset> data) {
		super(lamb, queue, manager, data);
	}

	@Override
	public AbstractQuery generateQuery() {
		//select 'projections' from 'dataset' where 'selections'
	    String queryID = count + "";
	    String queueID = queueId + "";
	    Dataset dataset = getRandomDataset();
	    ArrayList<Projection> projections = new ArrayList<Projection>();
	    ArrayList<Selection> selections = new ArrayList<Selection>();
	    int colNum = getRandomColNumber();
	    for(int i = 0; i < colNum; i++) {
	    	projections.add(getRandomProjection(dataset));
	    }
		
//	    for(int j = 0; j < colNum; j++) {
//	    	selections.add(getRandomSelection(dataset));
//	    }
	    
	    SingleTableQuery result = new SingleTableQuery(queryID, queueID, dataset, projections, selections);
	    System.out.println(result.toHiveQL(false));
	    count++;
		return result;
	}

}
