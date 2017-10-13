package com.caseystella.analysis.project;

import com.caseystella.analysis.vectorize.VectorizedPoint;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public enum Projectors {
  TSNE(c -> {
    Projector p = new TSNE();
    p.configure(c);
    return p;
  })
  ;
  Function<Map<String, Object>, Projector> creator;
  Projectors( Function<Map<String, Object>, Projector> creator) {
    this.creator = creator;
  }

  public List<VectorizedPoint> project( Map<String, Object> config
                                      , Iterable<VectorizedPoint> points
                                      , Map<String, Object> globalConfig
                                      ) {
    return creator.apply(config).project(points, globalConfig);
  }
}
