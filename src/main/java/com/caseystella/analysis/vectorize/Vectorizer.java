package com.caseystella.analysis.vectorize;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public interface Vectorizer {
  void configure(Map<String, Object> config);
  Iterable<VectorizedPoint>
    vectorize( Iterable<Tuple2<Map<String, Object>, List<Object>>> sample
             , JavaRDD<List<Object>> transformedRdd
             , Map<String, Object> globalConfig
             );
  boolean requiresFullDataset();
}
