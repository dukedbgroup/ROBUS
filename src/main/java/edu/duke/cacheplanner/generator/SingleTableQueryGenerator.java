package edu.duke.cacheplanner.generator;

import java.util.ArrayList;
import java.util.List;

import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.query.Projection;
import edu.duke.cacheplanner.query.Selection;
import edu.duke.cacheplanner.query.SingleTableQuery;
import edu.duke.cacheplanner.queue.ExternalQueue;

import edu.duke.cacheplanner.util.TruncatedNormal;

public class SingleTableQueryGenerator extends AbstractQueryGenerator {
  private int count = 0;
  
    public SingleTableQueryGenerator(double lamb, int id, double mean, double std) {
      super(lamb, id, mean, std);
  }
  
  
  public Dataset getRandomDataset() {
    //first look at the dataset distribution
    Random rand = new Random();
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    for(String d : queryDistribution.getQueueDistributionMap(queueId).keySet()) {
      cumulativeProb = cumulativeProb + queryDistribution.getDataProb(queueId, d);
      if(p <= cumulativeProb) {
        System.out.println("dataset: " + d + " is picked");
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

    //compute dicretized truncate normal distribution for column number
    for(int i = min; i < max + 1; i++) {
      double a = (double)i;
      double b = (double)i;
      if(i != min) { a = a - 0.5; }
      if(i != max) { b = b + 0.5; }
      distribution[i-1] = normal.cumulativeProbability(b) - normal.cumulativeProbability(a);
    }

    System.out.println(Arrays.toString(distribution));

    //pick column number used in the query with the distribution got from the above.
    Random rand = new Random();
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    for(int i = 0; i < distribution.length; i ++) {
      cumulativeProb = cumulativeProb + distribution[i];
      if(p <= cumulativeProb) {
        System.out.println("randomColNum : " + (i+1));
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
    System.out.println(Arrays.toString(columns));
    System.out.println(Arrays.toString((colDistribution)));

    Collection<Column> result = MathUtils.sampleWithoutReplacement(columns, colDistribution, numSamples);
    return new ArrayList<Column>(result);
  }
  
  public Selection getRandomSelection(Dataset data) {
    return null;
  }

  public Projection getRandomProjection(Dataset data) {
    return new Projection(AggregationFunction.SUM, getRandomColumns(data, 1).get(0));
  }

  @Override
  public AbstractQuery generateQuery() {
    //select 'projections' from 'dataset' where 'selections'
    List<Projection> projections = new ArrayList<Projection>();
    List<Selection> selections = new ArrayList<Selection>();

    String queryID = count + "";
    String queueID = queueId + "";

    //pick dataset
    Dataset dataset = getRandomDataset();

    //pick column number
    int colNum = getRandomColNumber(dataset);

    //sample columns that will used in query
    List<Column> columns = getRandomColumns(dataset, colNum);

    for(Column c : columns) {
      System.out.println(c.getColName() + " (" + c.getColumnType().toString() + ") picked!");
    }

    // 1. decide grouping / single
    // 2. if grouping, pick grouping column, pick projection(only aggregation allowed), pick selections
    // 2. if single, pick selection(randomly pick aggregation if number), projection(if number, if string put '') 

      
    SingleTableQuery result = new SingleTableQuery(queryID, queueID, dataset, projections, selections);
    count++;
    return result;
  }
  
}
