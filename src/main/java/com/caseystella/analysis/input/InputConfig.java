package com.caseystella.analysis.input;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import java.util.Map;

public class InputConfig {
  private Mode mode;
  private Map<String, Object> parserConfig;
  private String input;
  private String query;

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public Map<String, Object> getParserConfig() {
    return parserConfig;
  }

  public void setParserConfig(Map<String, Object> parserConfig) {
    this.parserConfig = parserConfig;
  }

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public DataFrame open(JavaSparkContext sc) {
    return getMode().open(getInput(), getQuery(), sc, getParserConfig());
  }
}
