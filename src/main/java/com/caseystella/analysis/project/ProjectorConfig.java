package com.caseystella.analysis.project;

import com.caseystella.analysis.vectorize.VectorizedPoint;

import java.util.List;
import java.util.Map;

public class ProjectorConfig {
  Projectors projector;
  Map<String, Object> config;

  public Projectors getProjector() {
    return projector;
  }

  public void setProjector(Projectors projector) {
    this.projector = projector;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }
  public List<VectorizedPoint> project(Iterable<VectorizedPoint> points
                                      , Map<String, Object> globalConfig
                                      ) {
    return getProjector().project(getConfig(), points, globalConfig);
  }
}
