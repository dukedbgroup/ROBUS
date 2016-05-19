package edu.duke.cacheplanner.query;

import java.io.Serializable;
import java.util.List;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;

/**
 * select 'grouping column', 'aggregations' from 'dataset' group by 'grouping column' where 'selections'
 * @author mayuresh
 */
@SuppressWarnings("serial")
public class GroupingQuery extends SingleDatasetQuery implements Serializable {

	Column groupingColumn;

  public GroupingQuery(int queryID, int queueId, Dataset dataset,
                          List<Projection> projections, List<Selection> selections,
                          Column grouping) {
	  super(queryID, queueId, dataset, projections, selections);
	  groupingColumn = grouping;
          setName(Constants.SALES_GROUP_QUERY);
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
	    String result = super.toHiveQL(cached);
 	    result = result + " GROUP BY " + groupingColumn.getColName();
	    return result;
	}

}

