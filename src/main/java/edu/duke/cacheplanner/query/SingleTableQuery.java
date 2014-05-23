/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.io.Serializable;
import java.util.List;

import edu.duke.cacheplanner.data.Dataset;

/**
 * select 'projections' from 'dataset' where 'selections'
 * @author mayuresh
 */
@SuppressWarnings("serial")
public class SingleTableQuery extends AbstractQuery implements Serializable {

	protected Dataset dataset;
	protected List<Projection> projections;
	protected List<Selection> selections;

	public SingleTableQuery(String queryID, String queueID, Dataset dataset,
			List<Projection> projections, List<Selection> selections) {
		this.QueryID = queryID;
		this.QueueID = queueID;
		setDataset(dataset);
		setProjections(projections);
		setSelections(selections);
	}


  /**
	 * @return the dataset
	 */
	public Dataset getDataset() {
		return dataset;
	}

	/**
	 * @return the projections
	 */
	public List<Projection> getProjections() {
		return projections;
	}

	/**
	 * @return the selections
	 */
	public List<Selection> getSelections() {
		return selections;
	}

	/**
	 * @param dataset the dataset to set
	 */
	protected void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}

	/**
	 * @param projections the projections to set
	 */
	protected void setProjections(List<Projection> projections) {
		//TODO: Make sure AggregationFunction is NONE for each projection
		this.projections = projections;
	}

	/**
	 * @param selections the selections to set
	 */
	protected void setSelections(List<Selection> selections) {
		this.selections = selections;
	}

	@Override
	public String toHiveQL(Boolean cached) {
    String result = "SELECT ";
    int count = 1;
    for(Projection projection: projections) {
      result = result + projection.toString();
      if(projections.size() != count) {
        result = result + ", ";
      }
      count ++;
    }
    result = result + " FROM ";
    if(cached) {
      result = result + dataset.getName() + "_cached";
    }
    else {
      result = result + dataset.getName();
    }
    count = 1;
    if(selections.size() > 0) {
      result = result + " WHERE ";
      for(Selection selection: selections) {
        result = result + selection.toString();
        if(selections.size() != count) {
          result = result + ", ";
        }
        count++;
      }
    }
		return result;
	}
}
