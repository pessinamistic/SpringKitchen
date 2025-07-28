/* (C) Lucipurr 69@420 */
package com.lucipurr.config;

import java.util.List;
import java.util.Properties;

public interface ConfigAccessor {

  public void createNodeString(String path, String data) throws Exception;

  public void createEphemeralNodeString(String path, String data) throws Exception;

  public String readString(String path) throws Exception;

  public void updateDataAsString(String path, String data) throws Exception;

  public void deleteNode(String path) throws Exception;

  public boolean exists(String path) throws Exception;

  public String getSharedLocksRoot();

  public List<String> getChildren(String path) throws Exception;

  public void loadProperties();

  public Properties properties();
}
