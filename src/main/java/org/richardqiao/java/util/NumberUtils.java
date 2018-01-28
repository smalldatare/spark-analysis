package org.richardqiao.java.util;

import scala.Double;

import java.math.BigDecimal;

/**
 * 数字格工具类
 * 
 * @author richardqiao
 *
 */
public class NumberUtils {

  /**
   * 格式化小数
   * 
   * @param num
   *            字符串
   * @param scale
   *            四舍五入的位数
   * @return 格式化小数
   */
  public static double formatDouble(double num, int scale) {
    try{
      if(num == Double.NaN() || num == Double.NegativeInfinity() || num == Double.PositiveInfinity()) return 0d;
      BigDecimal bd = new BigDecimal(num);
      return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }catch(Exception ex){
//      System.out.println("Error:" + num);
      ex.printStackTrace();
    }
    return 0d;
  }

}
