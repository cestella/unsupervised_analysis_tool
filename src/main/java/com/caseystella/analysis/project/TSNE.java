package com.caseystella.analysis.project;

import com.caseystella.analysis.util.Configurable;
import com.caseystella.analysis.vectorize.VectorizedPoint;
import com.jujutsu.tsne.barneshut.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TSNE implements Projector {
  private enum Option implements Configurable {
    PERPLEXITY("perplexity"),
    PARALLEL("parallel"),
    MAX_ITER("maxIter"),
    USE_PCA("usePca"),
    THETA("theta"),
    SILENT("silent")
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
  double perplexity;
  boolean parallel;
  int maxIter;
  boolean usePca;
  double theta;
  boolean silent;

  @Override
  public void configure(Map<String, Object> config) {
    perplexity = Option.PERPLEXITY.get(config, 40.0, Double.class);
    parallel = Option.PARALLEL.get(config, true, Boolean.class);
    maxIter = Option.MAX_ITER.get(config, 20000, Integer.class);
    usePca = Option.USE_PCA.get(config, true, Boolean.class);
    theta = Option.THETA.get(config, 0.5d, Double.class);
    silent = Option.SILENT.get(config, false, Boolean.class);
  }

  @Override
  public List<VectorizedPoint> project(Iterable<VectorizedPoint> sample, Map<String, Object> globalConfig) {
    List<VectorizedPoint> ret = null;
    int d = -1;
    if(sample instanceof List) {
      ret = (List)sample;
      d = ret.get(0).getVector().size();
    }
    else {
      ret = new ArrayList<>();
      for(VectorizedPoint pt : sample) {
        ret.add(pt);
        d = pt.getVector().size();
      }
    }
    double[][] X = new double[ret.size()][d];
    int i = 0;
    for(VectorizedPoint pt : ret) {
      double[] t = new double[pt.getVector().size()];
      for(int j = 0;j < pt.getVector().size();++i) {
        t[j] = pt.getVector().get(j);
      }
      X[i++] = t;
    }
    BarnesHutTSne tsne;
    boolean parallel = false;
    if(parallel) {
      tsne = new ParallelBHTsne();
    } else {
      tsne = new BHTSne();
    }
    TSneConfiguration tsneConfig = new TSneConfig(X, 2, d, perplexity, maxIter, usePca, theta, silent, true);
    double [][] Y = tsne.tsne(tsneConfig);
    for(i = 0;i < ret.size();++i) {
      ret.get(i).setProjection(Y[i]);
    }
    return null;
  }
}
