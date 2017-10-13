package com.caseystella.analysis.util;

import org.apache.metron.stellar.common.utils.ConversionUtils;

import java.util.Map;

public interface Configurable {
  default Object get(Map<String, Object> properties) {
      return properties.get(getName());
    }

  default public <T> T get(Map<String, Object> properties, T defaultVal, Class<T> clazz) {
      Object s = properties.get(getName());
      if(s == null) {
        return defaultVal;
      }
      T ret = ConversionUtils.convert(s, clazz);
      if(ret == null) {
        return defaultVal;
      }
      return ret;
    }

  default public boolean has(Map<String, Object> properties) {
      return properties.containsKey(getName());
  }

  String getName();
}
