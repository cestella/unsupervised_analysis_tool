package com.caseystella.analysis.input;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Map;

public interface InputHandler {
  DataFrame open(String inputName, String query, JavaSparkContext sc, Map<String, Object> properties);
}
