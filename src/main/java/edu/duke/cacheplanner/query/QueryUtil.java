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

  //TODO: change this to create the table to be cached
  public static String getCacheTableCreateSQL(String data, List<Column> columns) {
    String result = "CREATE TABLE " + data + "_cached (";
    for(Column col : columns) {
      result = result + col.getColName() + " " + col.getColumnType().toString() + ", ";
    }
    result = result.substring(0, result.length()-2);
    return result;
  }

  public static String getCacheTableInsertSQL(String data, List<Column> columns) {
    String result = "INSERT INTO TABLE " + data + "_cached SELECT ";
    for(Column col : columns) {
      result = result + col.getColName() + ", "; 
    } 
    result = result.substring(0, result.length()-2);
    result = result + " FROM " + data;
    return result;
  }
}
