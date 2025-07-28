/* (C) Lucipurr 69@420 */
package com.lucipurr.config;

import com.lucipurr.config.zookeeper.ZKWatcherCallBack;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;

@Slf4j
@Configuration
@EnableMBeanExport
public class ConfigFactory {

  @Getter
  @Value("${configmode:none}")
  private String configmode;

  @Value("${jsonfile:src/test/resources/appname-json.properties}")
  private String jsonfile;

  @Value("${watchFlag:true}")
  boolean watchFlag;

  @Value("${zkHost:localhost}")
  String zkHost;

  @Value("${sessionTimeout:10000}")
  int sessionTimeout;

  @Value("${environment:test}")
  private String environment;

  @Value("${appname:myapp}")
  private String appname;

  private String myIp;

  @Value("${serviceDetails:1234,abc}")
  private String serviceDetails;

  @Value("${serviceName:#{null}}")
  private String serviceName;

  @Value("${zkPort:2181}")
  private Integer zkPort;

  @Value("${appPort:#{null}}")
  private Integer appPort;

  @Value("${appSSlPort:#{null}}")
  private Integer appSSlPort;

  @Value("${uri:#{null}}")
  private String uri;

  @Autowired(required = false)
  private ZKWatcherCallBack zkWatcher;

  //  @Autowired(required = false)
  //  private ZookeeperServiceRegistry serviceRegistry;

  //  @Bean
  //  public ConfigAccessor getConfigurationAccessor() {
  //    log.info("Configuration accessor mode is: [{}]", configmode);
  //    ConfigAccessor configAccessor;
  //    switch (configmode) {
  //      case "zookeeper":
  //        try {
  //          InetAddress ipAddr = InetAddress.getLocalHost();
  //          myIp = ipAddr.getHostAddress();
  //        } catch (UnknownHostException ex) {
  //          log.error("Cannot connect to zookeeper", ex);
  //        }
  //        configAccessor =
  //            new ZooKeeperConfig(
  //                watchFlag,
  //                zkHost,
  //                sessionTimeout,
  //                environment,
  //                appname,
  //                myIp,
  //                serviceDetails,
  //                zkWatcher);
  //        log.info("Initialized zookeper config");
  //        if (serviceName != null) {
  //          serviceRegistry.init(zkHost, zkWatcher, environment);
  //          serviceRegistry.registerService(serviceName, uri, myIp, appSSlPort, appPort);
  //          log.info("Intialized zk config for service name: [{}]", serviceName);
  //        } else {
  //          log.error("Service name not provided");
  //        }
  //        break;
  //      case "json":
  //        configAccessor = new JsonConfig(jsonfile);
  //        log.info("Initialized json config");
  //        break;
  //      default:
  //        configAccessor = null;
  //        log.error("Invalid configmode: [{}]", configmode);
  //    }
  //    return configAccessor;
  //  }
}
