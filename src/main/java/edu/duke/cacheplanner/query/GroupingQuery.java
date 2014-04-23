package edu.duke.cacheplanner.query;

import java.util.List;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;

/**
 * select 'grouping column', 'aggregations' from 'dataset' group by 'grouping column' where 'selections'
 * @author mayuresh
 */
@SuppressWarnings("serial")
public class GroupingQuery extends SingleTableQuery {

	Column groupingColumn;
	List<Projection> aggregations;

	public GroupingQuery(String queryID, String queueID, Dataset dataset, 
			Column groupingColumn, List<Projection> aggregations, 
			List<Selection> selections) {
		super(queryID, queueID, dataset, aggregations, selections);
		aggregations.add(0, new Projection(
				AggregationFunction.NONE, groupingColumn));
		setProjections(aggregations);
	}

	/**
	 * @return the groupingColumn
	 */
	public Column getGroupingColumn() {
		return groupingColumn;
	}

	/**
	 * @param groupingColumn the groupingColumn to set
	 */
	protected void setGroupingColumn(Column groupingColumn) {
		this.groupingColumn = groupingColumn;
	}

	@Override
	public String toHiveQL(Boolean cached) {
		String groupByClause = "";
		return super.toHiveQL(cached) + groupByClause;
	}

}

