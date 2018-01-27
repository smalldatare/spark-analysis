package org.richardqiao.java.dao.impl;

import org.richardqiao.java.dao.ITop10CategoryDAO;
import org.richardqiao.java.domain.Top10Category;
import org.richardqiao.java.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 * 
 * @author richardqiao
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

  @Override
  public void insert(Top10Category category) {
    String sql = "insert into top10_category values(?,?,?,?,?)";

    Object[] params = new Object[] { category.getTaskid(), category.getCategoryid(), category.getClickCount(),
        category.getOrderCount(), category.getPayCount() };

    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeUpdate(sql, params);
  }

}
