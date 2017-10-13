package com.caseystella.analysis.stellar;

import org.apache.metron.stellar.common.StellarAssignment;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.common.generated.StellarParser;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.stellar.dsl.VariableResolver;
import org.apache.metron.stellar.dsl.functions.resolver.FunctionResolver;
import org.apache.spark.sql.Row;

import java.util.*;

public enum StellarExecutor {
  INSTANCE;
  private static ThreadLocal<StellarProcessor> processor = ThreadLocal.withInitial(() -> new StellarProcessor());

  public static class RowMap implements Map<String, Object> {
    Row r;
    public RowMap(Row r) {
      this.r = r;
    }

    @Override
    public int size() {
      return r.size();
    }

    @Override
    public boolean isEmpty() {
      return r == null?true:r.size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
      return !r.schema().getFieldIndex((String)key).isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public Object get(Object key) {
      if(r == null) {
        return null;
      }
      int i = r.fieldIndex(key.toString());
      return i >= 0 ? r.get(i) : null;
    }

    @Override
    public Object put(String key, Object value) {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public Object remove(Object key) {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public Set<String> keySet() {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }

    @Override
    public Collection<Object> values() {
      return null;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
      throw new UnsupportedOperationException("Unable to call this on a Row");
    }
  }

  private static class Resolver extends MapVariableResolver {

    public Resolver(Map variableMappingOne, Map... variableMapping) {
      super(variableMappingOne, variableMapping);
    }
    public Resolver(Map variableMappingOne, Map variableMappingTwo, Map... variableMapping) {
      super(variableMappingOne, variableMappingTwo);
      add(variableMapping);
    }
  }


  public Optional<Object> executeExpression(String expression, Row r, Context context, Map<String, Object>... extraVariables) throws Exception {
    VariableResolver resolver = new Resolver(new RowMap(r), extraVariables);
    return Optional.ofNullable(processor.get().parse(expression, resolver, StellarFunctions.FUNCTION_RESOLVER(), context));
  }

  public Optional<Object> executeExpressions(StellarExpressions expressions
                                            , Row r
                                            , Context context
                                            , Map<String, Object>... extraVariables
                                            ) throws Exception {
    Map<String, Object> tempVariables = new HashMap<>();
    VariableResolver resolver = new Resolver(new RowMap(r), tempVariables, extraVariables);
    for(String expression : expressions.getExpressions()) {
      StellarAssignment assignment = StellarAssignment.from(expression);
      Object result = processor.get().parse(assignment.getStatement(), resolver, StellarFunctions.FUNCTION_RESOLVER(), context);
      if(result != null) {
        tempVariables.put(assignment.getVariable(), result);
      }
    }
    return Optional.ofNullable(tempVariables.get(expressions.getVariable()));
  }
}
