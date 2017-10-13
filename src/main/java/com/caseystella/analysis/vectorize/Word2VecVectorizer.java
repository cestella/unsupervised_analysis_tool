package com.caseystella.analysis.vectorize;

import com.caseystella.analysis.util.Configurable;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Word2VecVectorizer implements Vectorizer{
  private enum Option implements Configurable {
    DIMENSION("dimension"),
    WINDOW_SIZE("windowSize"),
    MIN_OCCURRANCE("minOccurrance")
    ;
    String name;
    Option(String name) {
      this.name = name;
    }
    public String getName() {
      return name;
    }
  }

  int dimension;
  int windowSize;
  int minOccurrance;

  @Override
  public void configure(Map<String, Object> config) {
    dimension = Option.DIMENSION.get(config, 100, Integer.class);
    windowSize = Option.WINDOW_SIZE.get(config, 5 ,Integer.class);
    minOccurrance = Option.MIN_OCCURRANCE.get(config, 20, Integer.class);
  }

  @Override
  public Iterable<VectorizedPoint> vectorize( Iterable<Tuple2<Map<String, Object>, List<Object>>> sample
                                            , JavaRDD<List<Object>> transformedRdd
                                            , Map<String, Object> globalConfig
                                            )
  {
    JavaRDD<List<String>> text =
            transformedRdd.map(new Function<List<Object>, List<String>>() {
              @Override
              public List<String> call(List<Object> row) throws Exception {
                List<String> str = new ArrayList<>();
                Iterables.addAll(str, stringify(row));
                return str;
              }
            }).cache();
    Word2Vec word2Vec = new Word2Vec()
            .setVectorSize(dimension)
            .setSeed(0)
            .setWindowSize(windowSize)
            .setMinCount(minOccurrance);
    final Word2VecModel model = word2Vec.fit(text);
    return Iterables.transform(sample, new DocVectorize(model, dimension));
  }

  @Override
  public boolean requiresFullDataset() {
    return true;
  }

  private static Iterable<String> stringify(Iterable<Object> row) {
    return Iterables.transform(Iterables.filter(row, o -> o != null && !StringUtils.isEmpty(o.toString()))
                              , o -> o.toString()
                              );
  }

  private static class DocVectorize implements com.google.common.base.Function<Tuple2<Map<String, Object>, List<Object>>, VectorizedPoint> {
    Word2VecModel model;
    int dimension;

    public DocVectorize(Word2VecModel model, int dimension) {
      this.model=model;
      this.dimension = dimension;
    }
    @Nullable
    @Override
    public VectorizedPoint apply(@Nullable Tuple2<Map<String, Object>, List<Object>> in) {
      VectorizedPoint point = new VectorizedPoint();
      point.setData(in._1);
      List<Double> avgVec = new ArrayList<>();
      for(int i = 0;i < dimension;++i) {
        avgVec.set(i, 0d);
      }
      int n = 0;
      for(String word : stringify(in._2)) {
        Vector v = model.transform(word);
        for(int i = 0;i < dimension;++i) {
          avgVec.set(i, avgVec.get(i) + v.apply(i));
        }
        n++;
      }
      double sum = 0;
      for(int i = 0;i < dimension;++i) {
        double d = avgVec.get(i) / n;
        sum += d*d;
        avgVec.set(i, d);
      }
      double norm = Math.sqrt(sum);
      for(int i = 0;i < dimension;++i) {
        double d = avgVec.get(i) / norm;
        avgVec.set(i, d);
      }
      return point;
    }
  }
}
