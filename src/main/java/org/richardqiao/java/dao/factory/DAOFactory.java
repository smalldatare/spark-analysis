package org.richardqiao.java.dao.factory;

import org.richardqiao.java.dao.IAdBlacklistDAO;
import org.richardqiao.java.dao.IAdClickTrendDAO;
import org.richardqiao.java.dao.IAdProvinceTop3DAO;
import org.richardqiao.java.dao.IAdStatDAO;
import org.richardqiao.java.dao.IAdUserClickCountDAO;
import org.richardqiao.java.dao.IAreaTop3ProductDAO;
import org.richardqiao.java.dao.IPageSplitConvertRateDAO;
import org.richardqiao.java.dao.ISessionAggrStatDAO;
import org.richardqiao.java.dao.ISessionDetailDAO;
import org.richardqiao.java.dao.ISessionRandomExtractDAO;
import org.richardqiao.java.dao.ITaskDAO;
import org.richardqiao.java.dao.ITop10CategoryDAO;
import org.richardqiao.java.dao.ITop10SessionDAO;
import org.richardqiao.java.dao.impl.AdBlacklistDAOImpl;
import org.richardqiao.java.dao.impl.AdClickTrendDAOImpl;
import org.richardqiao.java.dao.impl.AdProvinceTop3DAOImpl;
import org.richardqiao.java.dao.impl.AdStatDAOImpl;
import org.richardqiao.java.dao.impl.AdUserClickCountDAOImpl;
import org.richardqiao.java.dao.impl.AreaTop3ProductDAOImpl;
import org.richardqiao.java.dao.impl.PageSplitConvertRateDAOImpl;
import org.richardqiao.java.dao.impl.SessionAggrStatDAOImpl;
import org.richardqiao.java.dao.impl.SessionDetailDAOImpl;
import org.richardqiao.java.dao.impl.SessionRandomExtractDAOImpl;
import org.richardqiao.java.dao.impl.TaskDAOImpl;
import org.richardqiao.java.dao.impl.Top10CategoryDAOImpl;
import org.richardqiao.java.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * 
 * @author richardqiao
 *
 */
public class DAOFactory {

  public static ITaskDAO getTaskDAO() {
    return new TaskDAOImpl();
  }

  public static ISessionAggrStatDAO getSessionAggrStatDAO() {
    return new SessionAggrStatDAOImpl();
  }

  public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
    return new SessionRandomExtractDAOImpl();
  }

  public static ISessionDetailDAO getSessionDetailDAO() {
    return new SessionDetailDAOImpl();
  }

  public static ITop10CategoryDAO getTop10CategoryDAO() {
    return new Top10CategoryDAOImpl();
  }

  public static ITop10SessionDAO getTop10SessionDAO() {
    return new Top10SessionDAOImpl();
  }

  public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
    return new PageSplitConvertRateDAOImpl();
  }

  public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
    return new AreaTop3ProductDAOImpl();
  }

  public static IAdUserClickCountDAO getAdUserClickCountDAO() {
    return new AdUserClickCountDAOImpl();
  }

  public static IAdBlacklistDAO getAdBlacklistDAO() {
    return new AdBlacklistDAOImpl();
  }

  public static IAdStatDAO getAdStatDAO() {
    return new AdStatDAOImpl();
  }

  public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
    return new AdProvinceTop3DAOImpl();
  }

  public static IAdClickTrendDAO getAdClickTrendDAO() {
    return new AdClickTrendDAOImpl();
  }

}
