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
        + ") ROW FORMAT delimited fields terminated by '|'"
        + " STORED AS TEXTFILE LOCATION "
        + "'" + data.getPath() + "'";
    return result;
  }

  public static String getCacheTableCreateSQL(Column column) {
    String result = "CREATE TABLE " + column.getDatasetName() + "_" + column.getColName() +  " (";
    result = result + column.getColName() + " " + column.getColumnType().toString() + ")";
    return result;
  }

  public static String getCacheTableInsertSQL(Column column) {
    String result = "INSERT INTO TABLE " + column.getDatasetName() + "_" + column.getColName() + " SELECT ";
    result = result + column.getColName();
    result = result + " FROM " + column.getDatasetName();
    return result;
  }
  
  public static String getDropTableSQL(String data) {
	  return "DROP TABLE IF EXISTS " + data;
  }
  
}
