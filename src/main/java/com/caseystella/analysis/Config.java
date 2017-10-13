package com.caseystella.analysis;

import com.caseystella.analysis.context.ContextConfig;
import com.caseystella.analysis.input.InputConfig;
import com.caseystella.analysis.project.ProjectorConfig;
import com.caseystella.analysis.stellar.StellarExpressions;
import com.caseystella.analysis.vectorize.VectorizerConfig;

import java.util.List;
import java.util.Map;

public class Config {
  InputConfig input;
  List<ContextConfig> context;
  StellarExpressions transformation;
  SampleConfig sample;
  VectorizerConfig vectorizer;
  ProjectorConfig projector;
  Map<String, Object> globalConfig;

  public InputConfig getInput() {
    return input;
  }

  public void setInput(InputConfig input) {
    this.input = input;
  }

  public List<ContextConfig> getContext() {
    return context;
  }

  public void setContext(List<ContextConfig> context) {
    this.context = context;
  }

  public StellarExpressions getTransformation() {
    return transformation;
  }

  public void setTransformation(StellarExpressions transformation) {
    this.transformation = transformation;
  }

  public SampleConfig getSample() {
    return sample;
  }

  public void setSample(SampleConfig sample) {
    this.sample = sample;
  }

  public VectorizerConfig getVectorizer() {
    return vectorizer;
  }

  public void setVectorizer(VectorizerConfig vectorizer) {
    this.vectorizer = vectorizer;
  }

  public ProjectorConfig getProjector() {
    return projector;
  }

  public void setProjector(ProjectorConfig projector) {
    this.projector = projector;
  }

  public Map<String, Object> getGlobalConfig() {
    return globalConfig;
  }

  public void setGlobalConfig(Map<String, Object> globalConfig) {
    this.globalConfig = globalConfig;
  }
}
