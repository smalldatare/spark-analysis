package org.richardqiao.java.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author richardqiao
 *
 */
public class ConfigurationManager {

  private static Properties prop = new Properties();

  /**
   */
  static {
    try {
      // 通过一个“类名.class”的方式，就可以获取到这个类在JVM中对应的Class对象
      // 然后再通过这个Class对象的getClassLoader()方法，就可以获取到当初加载这个类的JVM
      // 中的类加载器（ClassLoader），然后调用ClassLoader的getResourceAsStream()这个方法
      // 就可以用类加载器，去加载类加载路径中的指定的文件
      // 最终可以获取到一个，针对指定文件的输入流（InputStream）
      InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
      prop.load(in);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * @param key
   * @return value
   */
  public static String getProperty(String key) {
    return prop.getProperty(key);
  }

  /**
   *
   * @param key
   * @return value
   */
  public static Integer getInteger(String key) {
    String value = getProperty(key);
    try {
      return Integer.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  /**
   *
   * @param key
   * @return value
   */
  public static Boolean getBoolean(String key) {
    String value = getProperty(key);
    try {
      return Boolean.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   *
   * @param key
   * @return
   */
  public static Long getLong(String key) {
    String value = getProperty(key);
    try {
      return Long.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0L;
  }

}
