package com.caseystella.analysis;

import com.caseystella.analysis.context.Contextualizer;
import com.caseystella.analysis.input.InputConfig;
import com.caseystella.analysis.transform.Transformer;
import com.caseystella.analysis.vectorize.VectorizedPoint;
import com.caseystella.analysis.vectorize.Vectorizers;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.metron.stellar.common.utils.JSONUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class CLI {

  public static void main(String... argv) throws IOException {
    SparkConf conf = new SparkConf().setAppName("Summarizer");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    JavaSparkContext sc = new JavaSparkContext(conf);

    File configFile = new File(argv[0]);
    File outFile = new File(argv[1]);
    Config config = JSONUtils.INSTANCE.load(configFile, Config.class);
    DataFrame df = config.getInput().open(sc);
    Output out = compute(df, config);
    try(PrintWriter pw = new PrintWriter(outFile)) {
      IOUtils.write(JSONUtils.INSTANCE.toJSON(out, true), pw);
    }
  }

  public static Output compute(DataFrame df, Config config) {
    Output out = new Output();
    out.setColumns(df.columns());
    Map<String, Object> context = Contextualizer.INSTANCE.contextualize(df, config.getGlobalConfig(), config.getContext());
    JavaRDD<Tuple2<Map<String, Object>, List<Object>>> transformed =
            Transformer.INSTANCE.transform(df, config.getGlobalConfig(), config.getTransformation(), context);
    transformed.cache();

    JavaRDD<List<Object>> transformedValues = transformed.map(new Function<Tuple2<Map<String, Object>, List<Object>>, List<Object>>() {
      @Override
      public List<Object> call(Tuple2<Map<String, Object>, List<Object>> in) throws Exception {
        return in._2;
      }
    });

    Iterable<Tuple2<Map<String, Object>, List<Object>>> sample =
            transformed.sample(false, config.getSample().getFraction(), 0L)
                       .collect();
    if(config.getSample().getMaxRecords() != null) {
      sample = Iterables.limit(sample, config.getSample().getMaxRecords());
    }
    Iterable<VectorizedPoint> vectorizedPoints = config.getVectorizer()
                                                       .vectorize(sample, transformedValues, config.getGlobalConfig());
    List<VectorizedPoint> points = config.getProjector().project(vectorizedPoints, config.getGlobalConfig());
    out.setPoints(points);
    return out;
  }
}
