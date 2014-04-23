/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.util.List;

import edu.duke.cacheplanner.data.Dataset;

/**
 * select 'projections' from 'dataset' where 'selections'
 * @author mayuresh
 */
@SuppressWarnings("serial")
public class SingleTableQuery extends AbstractQuery {

	private Dataset dataset;
	private List<Projection> projections;
	private List<Selection> selections;

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
		// TODO Auto-generated method stub
		return null;
	}
}