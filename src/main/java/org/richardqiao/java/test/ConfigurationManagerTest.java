package org.richardqiao.java.test;

import org.richardqiao.java.conf.ConfigurationManager;

/**
 * 配置管理组件测试类
 * 
 * @author richardqiao
 *
 */
public class ConfigurationManagerTest {

  public static void main(String[] args) {
    String testkey1 = ConfigurationManager.getProperty("testkey1");
    String testkey2 = ConfigurationManager.getProperty("kafka.metadata.broker.list");
    System.out.println(testkey1);
    System.out.println(testkey2);
  }

}
