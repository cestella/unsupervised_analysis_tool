package com.caseystella.analysis.input;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import java.util.Map;

public enum Mode implements InputHandler{
   SQL(new SQLHandler())
  ,CSV(new CSVHandler())
  ;
  InputHandler handler;
  Mode(InputHandler handler) {
    this.handler = handler;
  }

  @Override
  public DataFrame open(String inputName, String query, JavaSparkContext sc, Map<String, Object> properties) {
    return handler.open(inputName, query, sc, properties);
  }
}
