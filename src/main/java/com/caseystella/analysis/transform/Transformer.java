package com.caseystella.analysis.transform;

import com.caseystella.analysis.stellar.StellarExecutor;
import com.caseystella.analysis.stellar.StellarExpressions;
import org.apache.metron.stellar.dsl.Context;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;

public enum Transformer {
  INSTANCE;

  public JavaRDD<Tuple2<Map<String, Object>, List<Object>>> transform( DataFrame df
                  , final Map<String, Object> globalConfig
                  , StellarExpressions expressions
                  , Map<String, Object> context
                  )
  {
    JavaRDD<Row> rdd = df.toJavaRDD();
    return rdd.flatMap(new MapFunction(globalConfig, expressions, context));
  }

  public static class MapFunction implements FlatMapFunction<Row, Tuple2<Map<String, Object>, List<Object>>> {
    Map<String, Object> globalConfig;
    StellarExpressions expressions;
    final Map<String, Object> context;
    Context stellarContext;

    public MapFunction(Map<String, Object> globalConfig, StellarExpressions expressions, final Map<String, Object> context) {
      this.globalConfig = globalConfig;
      this.expressions = expressions;
      this.context = context;
      stellarContext = new Context.Builder().with(Context.Capabilities.GLOBAL_CONFIG, () -> globalConfig).build();
    }

    @Override
    public Iterable<Tuple2<Map<String, Object>, List<Object>>> call(Row row) throws Exception {
      Optional<Object> r = StellarExecutor.INSTANCE.executeExpressions(expressions, row, stellarContext, context);
      List<Tuple2<Map<String, Object>, List<Object>>> ret = new ArrayList<>();
      Map<String, Object> obj = new HashMap<>();
      String[] fieldNames = row.schema().fieldNames();
      for(int i = 0;i < row.size();++i) {
        Object o = row.get(i);
        if(o != null) {
          obj.put(fieldNames[i], o);
        }
      }
      if(r.isPresent()) {
        ret.add(new Tuple2<>(obj, (List<Object>) r.get()));
        return ret;
      }
      return ret;
    }
  }
}
