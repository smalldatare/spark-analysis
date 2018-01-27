package org.richardqiao.java.dao;

import org.richardqiao.java.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * 
 * @author richardqiao
 *
 */
public interface ISessionRandomExtractDAO {

  /**
   * 插入session随机抽取
   * 
   * @param sessionAggrStat
   */
  void insert(SessionRandomExtract sessionRandomExtract);

}
