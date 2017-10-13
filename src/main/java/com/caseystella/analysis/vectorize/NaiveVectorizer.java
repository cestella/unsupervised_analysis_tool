package com.caseystella.analysis.vectorize;

import com.caseystella.analysis.util.Configurable;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NaiveVectorizer implements Vectorizer {
  private enum Option implements Configurable {
    UNIT_NORMALIZE("unitNormalize")
    ;
    String name;
    Option(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
  private boolean unitNormalize = false;

  @Override
  public void configure(Map<String, Object> properties) {
    unitNormalize = Option.UNIT_NORMALIZE.has(properties) && Option.UNIT_NORMALIZE.get(properties, false, Boolean.class);
  }

  @Override
  public Iterable<VectorizedPoint>
  vectorize(Iterable<Tuple2<Map<String, Object> , List<Object>>> sample
           , JavaRDD<List<Object>> transformedRdd
           , Map<String, Object> globalConfig)
  {
    return Iterables.transform( sample
                              , kv -> {
              VectorizedPoint ret = new VectorizedPoint();
              Map<String, Object> data = kv._1;
              ret.setData(data);
              double sum = 0d;
              List<Double> v = new ArrayList<>();
              for(Object o : kv._2) {
                double d = o == null?0:((Number)o).doubleValue();
                sum += d*d;
                v.add(d);
              }
              if(unitNormalize) {
                double norm_l2 = Math.sqrt(sum);
                for(int i = 0;i < v.size();++i) {
                  v.set(i, v.get(i)/norm_l2);
                }
              }
              ret.setVector(v);
              return ret;
            });
  }

  @Override
  public boolean requiresFullDataset() {
    return false;
  }


}
