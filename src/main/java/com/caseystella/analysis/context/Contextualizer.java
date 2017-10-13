package com.caseystella.analysis.context;

import com.caseystella.analysis.stellar.StellarExecutor;
import com.caseystella.analysis.stellar.StellarExpressions;
import org.apache.metron.stellar.dsl.Context;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;

public enum Contextualizer {
  INSTANCE;

  public Map<String, Object> contextualize(DataFrame df, final Map<String, Object> globalConfig, final List<ContextConfig> config) {
    Map<String, Object> ret = new HashMap<>();
    if(config.isEmpty()) {
      return ret;
    }
    JavaRDD<Row> rdd = df.toJavaRDD();

    JavaPairRDD<String, Tuple2<String, Object>> mapRdd =  rdd.flatMapToPair(new MapFunction(globalConfig, config));
    for(Tuple2<String, Tuple2<String, Object>> kv : mapRdd.reduceByKey(new ReduceFunction(globalConfig, config)).collect()) {
      ret.put(kv._2._1, kv._2._2);
    }
    return ret;
  }

  public static class ReduceFunction implements Function2<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>> {

    private Map<String, StellarExpressions> config;
    private Context stellarContext;
    public ReduceFunction(Map<String, Object> globalConfig, List<ContextConfig> config) {
      this.config = new HashMap<>();
      for(ContextConfig cc : config) {
        this.config.put(cc.getReduce().getVariable(), cc.getReduce());
      }
      stellarContext = new Context.Builder().with(Context.Capabilities.GLOBAL_CONFIG, () -> globalConfig).build();
    }

    @Override
    public Tuple2<String, Object> call(Tuple2<String, Object> s, Tuple2<String, Object> x) throws Exception {
      StellarExpressions expr = config.get(s._1);
      if(expr != null) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("left", s);
        variables.put("right", s);
        Optional<Object> r = StellarExecutor.INSTANCE.executeExpressions(expr, null, stellarContext, variables);
        if(r.isPresent()) {
          return new Tuple2<>(s._1, r.get());
        }
      }
      return null;
    }
  }

  public static class MapFunction implements PairFlatMapFunction<Row, String, Tuple2<String, Object>> {
    private List<ContextConfig> config;
    private Context stellarContext;
    public MapFunction(Map<String, Object> globalConfig, List<ContextConfig> config) {
      this.config = config;
      stellarContext = new Context.Builder().with(Context.Capabilities.GLOBAL_CONFIG, () -> globalConfig).build();
    }
    @Override
    public Iterable<Tuple2<String, Tuple2<String, Object>>> call(Row row) throws Exception {
      List<Tuple2<String, Tuple2<String, Object>>> ret = new ArrayList<>();
      for(ContextConfig cc : config ) {
        Optional<Object> r = StellarExecutor.INSTANCE.executeExpressions(cc.getMap(), row, stellarContext);
        if(r.isPresent()) {
          String variable = cc.getMap().getVariable();
          ret.add(new Tuple2<>(variable, new Tuple2<>(variable, r.get())));
        }
      }
      return ret;
    }
  }

}
