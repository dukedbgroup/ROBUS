package edu.duke.cacheplanner.data;

public class Column {
  
  double estimatedSize;
  String colName;
  String parentDataset;

  public Column(double size, String name, String parent) {
    estimatedSize = size;
    colName = name;
    parentDataset = parent;
  }
}
