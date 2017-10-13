package com.caseystella.analysis.project;

import com.caseystella.analysis.vectorize.VectorizedPoint;

import java.util.List;
import java.util.Map;

public interface Projector {
  void configure(Map<String, Object> config);
  List<VectorizedPoint> project(Iterable<VectorizedPoint> sample, Map<String, Object> globalConfig);
}
