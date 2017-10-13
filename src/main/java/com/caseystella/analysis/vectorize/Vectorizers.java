package com.caseystella.analysis.vectorize;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public enum Vectorizers {
  NAIVE(c -> {
    NaiveVectorizer v = new NaiveVectorizer();
    v.configure(c);
    return v;
  }),
  WORD2VEC(c -> {
    Word2VecVectorizer v = new Word2VecVectorizer();
    v.configure(c);
    return v;
  })
  ;
  Function<Map<String, Object>, Vectorizer> creator;
  Vectorizers(Function<Map<String, Object>, Vectorizer> creator) {
    this.creator = creator;
  }

  public Iterable<VectorizedPoint> vectorize( Map<String, Object> config
                                     , Iterable<Tuple2<Map<String, Object>, List<Object>>> sample
                                     , JavaRDD<List<Object>> transformedRdd
                                     , Map<String, Object> globalConfig
                                 )
  {
    return creator.apply(config).vectorize(sample, transformedRdd, globalConfig);
  }
}
