package org.richardqiao.java.dao.impl;

import org.richardqiao.java.dao.IPageSplitConvertRateDAO;
import org.richardqiao.java.domain.PageSplitConvertRate;
import org.richardqiao.java.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * 
 * @author richardqiao
 *
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

  @Override
  public void insert(PageSplitConvertRate pageSplitConvertRate) {
    String sql = "insert into page_split_convert_rate values(?,?)";
    Object[] params = new Object[] { pageSplitConvertRate.getTaskid(), pageSplitConvertRate.getConvertRate() };

    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeUpdate(sql, params);
  }

}
