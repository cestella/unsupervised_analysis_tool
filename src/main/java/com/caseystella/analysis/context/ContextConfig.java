package com.caseystella.analysis.context;

import com.caseystella.analysis.stellar.StellarExpressions;

public class ContextConfig {
  String filter;
  StellarExpressions map;
  StellarExpressions reduce;

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public StellarExpressions getMap() {
    return map;
  }

  public void setMap(StellarExpressions map) {
    this.map = map;
  }

  public StellarExpressions getReduce() {
    return reduce;
  }

  public void setReduce(StellarExpressions reduce) {
    this.reduce = reduce;
  }
}
