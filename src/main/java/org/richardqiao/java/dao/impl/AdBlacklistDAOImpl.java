package org.richardqiao.java.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.richardqiao.java.dao.IAdBlacklistDAO;
import org.richardqiao.java.domain.AdBlacklist;
import org.richardqiao.java.jdbc.JDBCHelper;

/**
 * 广告黑名单DAO实现类
 * 
 * @author richardqiao
 *
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {

  /**
   * 批量插入广告黑名单用户
   * 
   * @param adBlacklists
   */
  @Override
  public void insertBatch(List<AdBlacklist> adBlacklists) {
    String sql = "INSERT INTO ad_blacklist VALUES(?)";

    List<Object[]> paramsList = new ArrayList<Object[]>();

    for (AdBlacklist adBlacklist : adBlacklists) {
      Object[] params = new Object[] { adBlacklist.getUserid() };
      paramsList.add(params);
    }

    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeBatch(sql, paramsList);
  }

  /**
   * 查询所有广告黑名单用户
   * 
   * @return
   */
  @Override
  public Set<AdBlacklist> findAll() {
    String sql = "SELECT * FROM ad_blacklist";
    final Set<AdBlacklist> adBlacklists = new HashSet<AdBlacklist>();
    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
      @Override
      public void process(ResultSet rs) throws Exception {
        while (rs.next()) {
          long userid = Long.valueOf(String.valueOf(rs.getInt(1)));

          AdBlacklist adBlacklist = new AdBlacklist();
          adBlacklist.setUserid(userid);

          adBlacklists.add(adBlacklist);
        }
      }

    });

    return adBlacklists;
  }

}
