package edu.duke.cacheplanner.query;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;

import java.util.List;

public class QueryUtil {

	
  public String translateToHiveQL() {
    return "";
  }

  public static String getTableCreateSQL(Dataset data) {
    String result = "CREATE EXTERNAL TABLE IF NOT EXISTS " + data.getName() + "(";
    for(Column col : data.getColumns()) {
      result = result + col.getColName() + " " + col.getColumnType().toString() + ", ";
    }
    result = result.substring(0, result.length()-2)
        + ") ROW FORMAT delimited fields terminated by ','"
        + " STORED AS TEXTFILE LOCATION "
        + "'" + data.getPath() + "'";
    return result;
  }

  //TODO: change this to create the caching table
  public static String getCacheTableCreateSQL(List<Column> columns, Dataset data) {
    String result = "CREATE EXTERNAL TABLE IF NOT EXISTS " + data.getName() + "(";
    for(Column col : data.getColumns()) {
      result = result + col.getColName() + " " + col.getColumnType().toString() + ", ";
    }
    result = result.substring(0, result.length()-2)
            + ") ROW FORMAT delimited fields terminated by ','"
            + " STORED AS TEXTFILE LOCATION "
            + "'" + data.getPath() + "'";
    return result;
  }
}
