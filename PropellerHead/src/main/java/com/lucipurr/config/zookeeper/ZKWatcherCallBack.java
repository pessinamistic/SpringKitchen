/* (C) Lucipurr 69@420 */
package com.lucipurr.config.zookeeper;

import java.util.Properties;
import org.apache.zookeeper.WatchedEvent;

public interface ZKWatcherCallBack {

  public void callBack(String propertyName, String newValue);

  public void callBack(WatchedEvent event, String newValue);

  public void addCallBack(Properties properties);
}
