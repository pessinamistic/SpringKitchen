/* (C) Lucipurr 69@420 */
package com.lucipurr.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Configuration {

  @Autowired ConfigAccessor configAccessor;

  private static Configuration config;

  @PostConstruct
  public void initialize() {
    config = this;
    configAccessor.loadProperties();
    log.debug("Properties initialized:" + configAccessor.properties());
  }

  public void createEphemeralNode(String path, String data) throws Exception {
    configAccessor.createEphemeralNodeString(path, data);
  }

  public String get(String key) {
    return configAccessor.properties().getProperty(key);
  }

  public String get(String key, String defaultValue) {
    return configAccessor.properties().getProperty(key, defaultValue);
  }

  public boolean checkIfPathExists(String path) throws Exception {
    return configAccessor.exists(path);
  }

  public String getString(String key) {
    return configAccessor.properties().getProperty(key).toString();
  }

  public int getInt(String key) {
    return Integer.parseInt(getString(key));
  }

  public int getInt(String key, int defaultValue) {
    return Integer.parseInt(get(key, "" + defaultValue));
  }

  public long getLong(String key) {
    return Long.parseLong(getString(key));
  }

  public double getDouble(String key) {
    return Double.parseDouble(get(key, "0"));
  }

  public double getDouble(String key, double defaultValue) {
    return Double.parseDouble(get(key, "" + defaultValue));
  }

  public long getLong(String key, long defaultValue) {
    return Long.parseLong(get(key, "" + defaultValue));
  }

  public static Configuration getConfig() {
    return config;
  }
}
