/* (C) Lucipurr 69@420 */
package com.lucipurr.config.json;

import com.lucipurr.config.ConfigAccessor;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @Profile(BaseConfig.CONFIG_TYPE_FILE)
public class JsonConfig implements ConfigAccessor {

  private static Logger logger = LoggerFactory.getLogger(JsonConfig.class);

  private String filename;

  Properties properties = new Properties();

  public JsonConfig(String filename) {
    this.filename = filename;
  }

  @Override
  public void createNodeString(String path, String data) throws Exception {
    ConfigurationUtil.writeOrUpdateJson(path, data, filename, false);
  }

  @Override
  public void createEphemeralNodeString(String path, String data) throws Exception {
    ConfigurationUtil.writeOrUpdateJson(path, data, filename, false);
  }

  @Override
  public String readString(String path) throws Exception {
    String data = ConfigurationUtil.readFile(path, filename);
    return data;
  }

  @Override
  public void updateDataAsString(String path, String data) throws Exception {
    ConfigurationUtil.writeOrUpdateJson(path, data, filename, false);
  }

  @Override
  public void deleteNode(String path) throws Exception {
    JSONObject obj = ConfigurationUtil.getJsonObject(filename);
    obj.remove(path);
    ConfigurationUtil.writeFile(obj, filename, false);
  }

  @Override
  public boolean exists(String path) throws IOException {
    JSONObject obj = ConfigurationUtil.getJsonObject(filename);
    return obj.has(path);
  }

  @Override
  public void loadProperties() {
    Properties properties = new Properties();
    try {
      JSONObject propJson = ConfigurationUtil.getJsonObject(filename, true);
      Iterator<String> it = propJson.keys();
      while (it.hasNext()) {
        String propName = it.next();
        String propValue = propJson.getString(propName);
        properties.put(propName, propValue);
      }
      this.properties = properties;
    } catch (Exception e) {
      logger.error("Exception in zk load props.", e);
    }
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public String getSharedLocksRoot() {
    return filename;
  }

  @Override
  public List<String> getChildren(String path) throws Exception {
    return null;
  }
}
