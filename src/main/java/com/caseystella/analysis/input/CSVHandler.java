package com.caseystella.analysis.input;

import com.caseystella.analysis.util.Configurable;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class CSVHandler implements InputHandler {
  public enum Options implements Configurable {
    QUERY("query")
    ,HAS_HEADER("hasHeader")
    ,INFER_SCHEMA("inferSchema" )
    ,EXPLICIT_SCHEMA("withSchema" )
    ;
    String optionName;
    Options(String optionName) {
      this.optionName = optionName;
    }
    public String getName() {
      return optionName;
    }
  }
  public enum SqlTypes {
    STRING(DataTypes.StringType),
    INTEGER(DataTypes.IntegerType),
    DATE(DataTypes.DateType),
    DOUBLE(DataTypes.DoubleType),
    FLOAT(DataTypes.FloatType),
    LONG(DataTypes.LongType);
    DataType dt;
    SqlTypes(DataType dt) {
      this.dt = dt;
    }

    public DataType getDataType() {
      return dt;
    }
  }
  private StructType customSchema(Map<String, String> schemaDef) {
    List<Tuple2<String, DataType>> schema = new ArrayList<>();
    for(Map.Entry<String, String> kv : schemaDef.entrySet()) {
      schema.add(new Tuple2<>(kv.getKey(), SqlTypes.valueOf(kv.getValue()).getDataType()));
    }

    StructField[] fields = new StructField[schema.size()];
    for(int i = 0;i < schema.size();++i) {
      Tuple2<String, DataType> s = schema.get(i);
      fields[i] = new StructField(s._1, s._2, true, Metadata.empty());
    }
    return new StructType(fields);
  }

  @Override
  public DataFrame open(String inputName, String query, JavaSparkContext sc, Map<String, Object> properties) {
    SQLContext sqlContext = new SQLContext(sc);
    DataFrameReader reader = sqlContext.read()
                                       .format("com.databricks.spark.csv")
                                       .option("header", Options.HAS_HEADER.get(properties, "true", String.class))
                                       .option("inferSchema", Options.INFER_SCHEMA.get(properties, "true", String.class))
                                       ;
    if(Options.EXPLICIT_SCHEMA.has(properties)) {
      reader = reader.schema(customSchema(Options.EXPLICIT_SCHEMA.get(properties, null, Map.class)));
    }
    DataFrame df = reader.load(inputName);
    if(query != null) {
      String tableName = Iterables.getFirst(Splitter.on('.').split(Iterables.getLast(Splitter.on('/').split(inputName), inputName)), inputName);
      System.out.println("Registering " + tableName + "...");
      df.registerTempTable(tableName);
      return df.sqlContext().sql(Options.QUERY.get(properties, null, String.class));
    }
    return df;
  }
}
