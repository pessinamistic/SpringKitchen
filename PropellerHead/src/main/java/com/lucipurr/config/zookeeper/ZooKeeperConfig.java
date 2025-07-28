/* (C) Lucipurr 69@420 */
package com.lucipurr.config.zookeeper;

import com.lucipurr.config.ConfigAccessor;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Properties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperConfig implements ConfigAccessor, Watcher {

  private static final String DATAVERSION = "/dataversion";

  private static final String LOCKS = "/locks";

  private static Logger logger = LoggerFactory.getLogger(ZooKeeperConfig.class);

  private boolean watchFlag;

  private String environment;

  private String appname;

  private String myIp;

  private String serviceDetails;

  private CuratorFramework client;

  public static final String CONFIG_ROOT = "/configuration";

  public static final String SERVICE_ROOT = "/services";

  public static final String ENABLED = "enabled";

  public static final String DISABLED = "disabled";

  private ZKWatcherCallBack zkWatcher;

  private String envServiceRoot;
  private String envConfigRoot;
  private String sharedLocksRoot;
  private String configVersionsRoot;

  private String connectString;

  public boolean isWatchFlag() {
    return watchFlag;
  }

  public ZooKeeperConfig(
      boolean watchFlag,
      String connectStrings,
      int sessionTimeout,
      String environment,
      String appname,
      String myIp,
      String serviceDetails,
      ZKWatcherCallBack zkWatcher) {
    try {
      client =
          CuratorFrameworkFactory.newClient(connectStrings, new ExponentialBackoffRetry(1000, 3));
      client.start();
      try {
        client.checkExists().forPath("/");
        // zooKeeper.exists("/", false);
      } catch (Exception e) {
        logger.warn("", e);
      }
      this.environment = environment;
      this.appname = appname;
      this.watchFlag = watchFlag;
      this.myIp = myIp;
      this.connectString = connectStrings;
      this.serviceDetails = serviceDetails;
      this.zkWatcher = zkWatcher;
      envServiceRoot = "/" + environment + SERVICE_ROOT + "/" + appname;
      envConfigRoot = "/" + environment + CONFIG_ROOT + "/" + appname;
      sharedLocksRoot = "/" + environment + LOCKS + "/" + appname;
      configVersionsRoot = "/" + environment + DATAVERSION + "/" + appname;
      registerAsAServiceOnZK();
    } catch (Exception e) {
      logger.error("Exception in zkConfig constructor:", e);
    }
  }

  /**
   * @param path
   * @param data
   * @throws Exception
   * @throws KeeperExceptionwatch
   */
  private void createNodeByte(String path, byte[] data) throws Exception {
    // TODO: Need to implement CREATOR_ALL_ACL for authentication
    boolean ret = createRecursivly(path);
    logger.debug("return value :: " + ret);
    updateDataAsByte(path, data);
  }

  /**
   * @param path
   * @param data
   * @throws Exception
   */
  public void createNodeString(String path, String data) throws Exception {
    byte[] byteData = data.getBytes();
    Stat status = client.checkExists().forPath(path);
    if (status != null) {
      logger.debug("update data as byte");
      updateDataAsByte(path, byteData);
    } else {
      logger.debug("create node data byte");
      createNodeByte(path, byteData);
    }
  }

  private void createEphemeralNodeByte(String path, byte[] data) throws Exception {
    boolean ret = createEphemeralNodeRecursivly(path);
    logger.debug("return value :: " + ret);
  }

  public void createEphemeralNodeString(String path, String data) throws Exception {
    byte[] byteData = data.getBytes();
    logger.debug("create node data byte");
    createEphemeralNodeByte(path, byteData);
  }

  /**
   * @param path
   * @return
   * @throws Exception
   */
  public String readString(String path) throws Exception {
    byte[] data = readByte(path);
    return new String(data);
  }

  /**
   * @param path
   * @param data
   * @throws Exception
   */
  public void updateDataAsString(String path, String data) throws Exception {
    byte[] byteData = data.getBytes();
    updateDataAsByte(path, byteData);
  }

  /**
   * @param path
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void deleteNode(String path) throws InterruptedException, KeeperException {
    try {
      Stat status = client.checkExists().forPath(path);
      client.delete().withVersion(status.getVersion()).forPath(path);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      logger.error("Exception in deleting node at zk", e);
    }
  }

  /**
   * @param path
   * @return
   * @throws Exception
   */
  public List<String> getChildren(String path) throws Exception {
    List<String> znodeList = null;

    znodeList = client.getChildren().usingWatcher(this).forPath(path);

    return znodeList;
  }

  /**
   * @param path
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean exists(String path) throws Exception {

    if (client.checkExists().forPath(path) != null) {
      return true;
    } else {
      return false;
    }
  }

  private byte[] readByte(String path) throws Exception {
    Stat status = client.checkExists().usingWatcher(this).forPath(path);
    byte[] data = client.getData().storingStatIn(status).usingWatcher(this).forPath(path);
    return data;
  }

  private void updateDataAsByte(String path, byte[] data) throws Exception {
    Stat status = client.checkExists().forPath(path);
    client.setData().withVersion(status.getVersion()).forPath(path, data);
  }

  @PreDestroy
  public void cleanup() {
    if (client == null || client.getState() == CuratorFrameworkState.STOPPED) {
      logger.debug("-----------------------ZK already closed----------------------------");
      return;
    }
    try {
      client.close();
      logger.debug("-------------------------Closed ZK connection------------------------");
    } catch (Exception e) {
      logger.error("Exception in cleanup.", e);
    }
  }

  public void deRegisterServiceFromZookeeper() {
    try {
      updateDataAsString(getEnvServiceRoot() + "/" + myIp, DISABLED + "," + serviceDetails);
    } catch (Exception e) {
      logger.error("Exception in deregistration of service.", e);
    }
  }

  public void setWatchFlag(boolean watchFlag) {
    this.watchFlag = watchFlag;
  }

  Properties properties = new Properties();

  @Override
  public void loadProperties() {
    Properties properties1 = new Properties();
    try {
      String appPath = getEnvConfigRoot();
      List<String> props = getChildren(appPath);
      for (String propName : props) {
        String propPath = appPath + "/" + propName;
        try {
          String propValue = readString(propPath);
          properties1.put(propName, propValue);
        } catch (NoNodeException nne) {
          logger.info("No property on zk with name:" + propName);
        }
      }
      properties = properties1;
      logger.info("Properties loaded:" + properties);
    } catch (Exception e) {
      logger.error("Exception in zk load props:", e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      if (event.getPath() != null && event.getPath().contains(getEnvConfigRoot())) {
        logger.info(
            "Node changed:"
                + event.getPath()
                + " , "
                + event.getState()
                + " , "
                + event.toString());
        if (watchFlag) {
          logger.debug("Watchflag true");
          client.checkExists().watched().forPath(event.getPath());
        } else {
          logger.debug("Watchflag false");
          client.checkExists().forPath(event.getPath());
        }
        if (!event.getPath().equalsIgnoreCase(getEnvConfigRoot())) {
          // do not load all properties. load only the one that is changed/deleted.
          try {
            String propertyValue = readString(event.getPath());
            properties().setProperty(getPropertyName(event.getPath()), propertyValue);
            if (zkWatcher != null) {
              zkWatcher.callBack(getPropertyName(event.getPath()), propertyValue);
              zkWatcher.callBack(event, propertyValue);
            }
          } catch (KeeperException.NoNodeException nne) {
            // if the property is deleted. it should come here.
            logger.debug("Changed node is deleted." + event.getPath());
            properties().remove(getPropertyName(event.getPath()));
            if (zkWatcher != null) {
              zkWatcher.callBack(getPropertyName(event.getPath()), null);
              zkWatcher.callBack(event, null);
            }
          }
        } else {
          logger.debug("app root node updated or some property is added/deleted.");
          // app root node changed or some property is added/deleted.
          loadProperties();
          if (zkWatcher != null) {
            zkWatcher.addCallBack(this.properties);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Exception in watcher.", e);
    }
  }

  private String getEnvConfigRoot() {
    return envConfigRoot;
  }

  private String getPropertyName(String fullNodePath) {
    String propertyName = null;
    try {
      String tokens[] = fullNodePath.split("/");
      return tokens[tokens.length - 1];
    } catch (Exception e) {
      logger.error("Exception in getPropertyName.", e);
    }
    return propertyName;
  }

  private void registerAsAServiceOnZK() {
    try {

      String path = getEnvServiceRoot() + "/" + myIp;
      logger.debug(
          "Path in registerasservice" + path + " ::: and service details" + serviceDetails);
      createRecursivly(path);
      updateDataAsString(path, ENABLED + "," + serviceDetails);
    } catch (Exception e) {
      logger.error("Exception in registerAsAServiceOnZK.", e);
    }
  }

  public String getEnvServiceRoot() {
    return envServiceRoot;
  }

  public String getEnvironment() {
    return environment;
  }

  public String getAppname() {
    return appname;
  }

  public String getSharedLocksRoot() {
    return sharedLocksRoot;
  }

  public boolean createRecursivly(String path) throws Exception {
    if (path.length() == 0) return true;
    try {
      if (client.checkExists().forPath(path) != null) {
        logger.debug("path is not null" + path);
        return true;
      } else {
        logger.debug("creating path" + path);
        String temp = path.substring(0, path.lastIndexOf("/"));
        if (createRecursivly(temp)) {
          client.create().withACL(Ids.OPEN_ACL_UNSAFE).forPath(path, null);
        } else return false;
        return true;
      }
    } catch (NodeExistsException e) {
      return true;
    } catch (KeeperException e) {
      logger.warn("", e);
    } catch (InterruptedException e) {
      logger.warn("", e);
    }
    return false;
  }

  public boolean createEphemeralNodeRecursivly(String path) throws Exception {
    if (path.length() == 0) return true;
    try {
      if (client.checkExists().forPath(path) != null) {
        logger.debug("path is not null" + path);
        return true;
      } else {
        logger.debug("creating path" + path);
        String temp = path.substring(0, path.lastIndexOf("/"));
        if (createRecursivly(temp)) {
          client.create().withProtection().withMode(CreateMode.EPHEMERAL).forPath(path);
        } else return false;
        return true;
      }
    } catch (NodeExistsException e) {
      return true;
    } catch (KeeperException e) {
      logger.warn("", e);
    } catch (InterruptedException e) {
      logger.warn("", e);
    }
    return false;
  }

  public String getConnectString() {
    return connectString;
  }

  public String getConfigVersionsRoot() {
    return configVersionsRoot;
  }

  public CuratorFramework getClient() {
    return client;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  private void createPropertyNodeStructure() {
    try {
      if (!exists(ZooKeeperConfig.CONFIG_ROOT))
        createNodeString(ZooKeeperConfig.CONFIG_ROOT, "configuration");
      if (!exists("/" + environment + ZooKeeperConfig.CONFIG_ROOT))
        createNodeString("/" + environment + ZooKeeperConfig.CONFIG_ROOT, "environment");
      if (!exists("/" + environment + ZooKeeperConfig.CONFIG_ROOT + "/" + appname))
        createNodeString(
            "/" + environment + ZooKeeperConfig.CONFIG_ROOT + "/" + appname, "appname");
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }
}
