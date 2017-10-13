package com.caseystella.analysis.vectorize;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class VectorizerConfig {
  Vectorizers vectorizer;
  Map<String, Object> config;

  public Vectorizers getVectorizer() {
    return vectorizer;
  }

  public void setVectorizer(Vectorizers vectorizer) {
    this.vectorizer = vectorizer;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public Iterable<VectorizedPoint> vectorize( Iterable<Tuple2<Map<String, Object>, List<Object>>> sample
                                            , JavaRDD<List<Object>> transformedRdd
                                            , Map<String, Object> globalConfig
                                            ) {
    return getVectorizer().vectorize(getConfig(), sample, transformedRdd, globalConfig);
  }
}
