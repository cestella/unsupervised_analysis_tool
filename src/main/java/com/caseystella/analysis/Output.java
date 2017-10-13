package com.caseystella.analysis;

import com.caseystella.analysis.vectorize.VectorizedPoint;

import java.util.List;

public class Output {
  String[] columns;
  List<VectorizedPoint> points;

  public String[] getColumns() {
    return columns;
  }

  public void setColumns(String[] columns) {
    this.columns = columns;
  }

  public List<VectorizedPoint> getPoints() {
    return points;
  }

  public void setPoints(List<VectorizedPoint> points) {
    this.points = points;
  }
}
