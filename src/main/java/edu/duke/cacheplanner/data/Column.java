package edu.duke.cacheplanner.data;

import java.io.Serializable;

public class Column implements Serializable {

  String colName;
  String datasetName;
  /**
   * Store statistics on the column here.
   */
  double estimatedSize;
  ColumnType columnType;

  public Column(double size, String name, ColumnType type, String data) {
    estimatedSize = size;
    colName = name;
    columnType = type;
    datasetName = data;
  }

  public double getEstimatedSize() {
    return estimatedSize;
  }

  public void setEstimatedSize(double estimatedSize) {
    this.estimatedSize = estimatedSize;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    Column other = (Column) obj;
    return (this.colName == other.colName);
  }
}