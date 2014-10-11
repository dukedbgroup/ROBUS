package edu.duke.cacheplanner.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.ColumnType;
import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.listener.QuerySerialize;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.query.AggregationFunction;
import edu.duke.cacheplanner.query.GroupingQuery;
import edu.duke.cacheplanner.query.Projection;
import edu.duke.cacheplanner.query.Selection;
import edu.duke.cacheplanner.query.SelectionOperator;
import edu.duke.cacheplanner.query.SingleDatasetQuery;
import edu.duke.cacheplanner.util.TruncatedNormal;
import edu.umbc.cs.maple.utils.MathUtils;

/**
 * Single Table Query Generator
 *
 * @author Seunghyun Lee
 */
public class SingleTableQueryGenerator extends AbstractQueryGenerator {
  private int count = 0;
  protected static double meanColNum;
  protected static double stdColNum;
  protected double waitingTime;
  protected double groupingQueryProb;

  public SingleTableQueryGenerator(double lamb, int id, String name, 
		  double mean, double std, double grouping) {
	  super(lamb, id, name);
	  meanColNum = mean;
	  stdColNum = std;
	  waitingTime = 0.0;
	  groupingQueryProb = grouping;
  }
  
  
  public Dataset getRandomDataset() {
    //first look at the dataset distribution
    Random rand = new Random();
   
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    for(String d : queryDistribution.getQueueDistributionMap(queueId).keySet()) {
      cumulativeProb = cumulativeProb + queryDistribution.getDataProb(queueId, d);
      if(p <= cumulativeProb) {
        return getDataset(d);
      }
    }
    
    //error
    return null;
  }
  
  public int getRandomColNumber(Dataset dataset) {
    int min = 1;
    int max = dataset.getColumns().size();
    TruncatedNormal normal = new TruncatedNormal(meanColNum, stdColNum, min, max);
    double[] distribution = new double[max];

    //compute discretized truncate normal distribution for column number
    for(int i = min; i < max + 1; i++) {
      double a = (double)i;
      double b = (double)i;
      if(i != min) { a = a - 0.5; }
      if(i != max) { b = b + 0.5; }
      distribution[i-1] = normal.cumulativeProbability(b) - normal.cumulativeProbability(a);
    }

    //pick column number used in the query with the distribution got from the above.
    Random rand = new Random();
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    for(int i = 0; i < distribution.length; i ++) {
      cumulativeProb = cumulativeProb + distribution[i];
      if(p <= cumulativeProb) {
        return i + 1;
      }
    }
    return -1;
  }
    
  public List<Column> getRandomColumns(Dataset data, int numSamples) {
    int size = data.getColumns().size();
    Column[] columns = new Column[size];
    double[] colDistribution = new double[size];
    int pos = 0;
    for(Column c : data.getColumns()) {
      columns[pos] = c;
      colDistribution[pos] = queryDistribution.getColProb(queueId, data.getName(), c.getColName());
      pos++;
    }
    Collection<Column> result = MathUtils.sampleWithoutReplacement(columns, colDistribution, numSamples);
    return new ArrayList<Column>(result);
  }

  /**
   * Temporarily disabling random selection
   * TODO: use random selection when we know max and min over a column, ideally a histogram
   * @return
   */
  public SelectionOperator getRandomSelectionOperator() {
//    Random rand = new Random();
    int operatorNum = 1;//rand.nextInt(3);
    switch(operatorNum) {
      case 0 : return SelectionOperator.EQUAL;
      case 1 : return SelectionOperator.GREATER;
      case 2 : return SelectionOperator.LESSER;
    }
    return null;
  }

  /**
   * Temporarily disabling random selection
   * TODO: use random selection when we know max and min over a column, ideally a histogram
   * @return
   */
  public String getRandomValue(Column column) {
    Random rand = new Random();
    ColumnType type = column.getColumnType();
    if(type == ColumnType.BOOLEAN) {
      if(rand.nextInt(2) == 0) {
        return "true";
      }
      else {
        return "false";
      }
    }
    else if(type == ColumnType.DOUBLE) {
      double value = rand.nextDouble()*rand.nextInt(1000);
      return value + "";
    }
    else if(type == ColumnType.FLOAT) {
      float value = rand.nextFloat()*rand.nextInt(1000);
      return value + "";
    }
    else if(type == ColumnType.INT) {
      return rand.nextInt(1000) + "";
    }
    else if(type == ColumnType.STRING) {
      char c = (char) (rand.nextInt(26) + 'a');
      return c + "";
    }
    else if (type == ColumnType.TIMESTAMP) {
      int year = rand.nextInt(25) + 1990; //1990~2014
      int month = rand.nextInt(12)+1;
      int date = 0;
      if(month == 2) {
        date = rand.nextInt(28)+1;
      }
      else if(month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) {
        date = rand.nextInt(31)+1;
      }
      else {
        date = rand.nextInt(30)+1;
      }
      String dateString = date + "";
      if(date < 10) {
        dateString = "0" + dateString;
      }

      String monthString = month + "";
      if(month < 10) {
        monthString = "0" + monthString;
      }
      return year + "-" + monthString + "-" + dateString;
    }
    return null;
  }

  public AggregationFunction getRandomAggregationFunction(Column col, boolean useAggregation) {
    if(useAggregation) {
      if(isNumber(col)) {
        Random rand = new Random();
        int func = rand.nextInt(4);
        switch(func) {
          case 0: return AggregationFunction.COUNT;
          case 1: return AggregationFunction.MAX;
          case 2: return AggregationFunction.MIN;
          case 3: return AggregationFunction.SUM;
        }
      }
      else {
        return AggregationFunction.COUNT;
      }
    }
    return AggregationFunction.NONE;
  }

  public boolean isNumber(Column col) {
    ColumnType type = col.getColumnType();
    if(type == ColumnType.INT || type == ColumnType.FLOAT || type == ColumnType.DOUBLE) {
      return true;
    }
    return false;
  }

  public List<Column> uniformSampleColumns(List<Column> columns, int num) {
    List<Column> result = new ArrayList<Column>();
    Random rand = new Random();
    int count = 0;
    while(count < num) {
      Column candidate = columns.get(rand.nextInt(columns.size()));
      if(!result.contains(candidate)) {
        result.add(candidate);
      }
      count++;
    }
    return result;
  }

  public Selection getRandomSelection(Column column) {
    return new Selection(column, getRandomValue(column), getRandomSelectionOperator());
  }

  public Projection getRandomProjection(Column column, boolean useAggregation) {
    return new Projection(getRandomAggregationFunction(column, useAggregation), column);
  }

  @Override
  public AbstractQuery generateQuery() {
    //select 'projections' from 'dataset' where 'selections'

    int queryID = count;

    //pick dataset
    Dataset dataset = getRandomDataset();

    //pick column number
    int colNum = getRandomColNumber(dataset);

    //sample columns that will used in query
    List<Column> columns = getRandomColumns(dataset, colNum);

    // 1. decide grouping / single
    Random rand = new Random();
    //int randPick = rand.nextInt(2);
    List<Projection> projections = new ArrayList<Projection>();
    List<Selection> selections = new ArrayList<Selection>();

    //single table query is picked
    if(groupingQueryProb < rand.nextDouble()) {
      int selectionNum = rand.nextInt(columns.size()+1);

      List<Column> selectionCandidate = uniformSampleColumns(columns, selectionNum);

      for(Column col: columns) {
        projections.add(getRandomProjection(col, false));
      }

      for(Column col: selectionCandidate) {
        selections.add(getRandomSelection(col));
      }
      count++;
      AbstractQuery query = new SingleDatasetQuery(queryID, queueId, dataset, projections, selections);
      query.setWeight(externalQueue.getWeight());
      return query;
    }
    //grouping column
    else {
      int selectionNum = rand.nextInt(columns.size()+1);
      List<Column> selectionCandidate = uniformSampleColumns(columns, selectionNum);

      Column groupingCol = uniformSampleColumns(columns, 1).get(0);
      columns.remove(groupingCol);
      projections.add(new Projection(AggregationFunction.NONE, groupingCol));
      for(Column col: columns) {
        projections.add(getRandomProjection(col, true));
      }

      for(Column col : selectionCandidate) {
        selections.add(getRandomSelection(col));
      }
      count++;

      return new GroupingQuery(queryID, queueId, dataset, projections, selections, groupingCol);
    }
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
