package com.caseystella.analysis.vectorize;

import java.util.List;
import java.util.Map;

public class VectorizedPoint {
  Map<String, Object> data;
  List<Double> vector;
  double[] projection;

  public Map<String, Object> getData() {
    return data;
  }

  public void setData(Map<String, Object> data) {
    this.data = data;
  }

  public List<Double> getVector() {
    return vector;
  }

  public void setVector(List<Double> vector) {
    this.vector = vector;
  }

  public double[] getProjection() {
    return projection;
  }

  public void setProjection(double[] projection) {
    this.projection = projection;
  }
}
