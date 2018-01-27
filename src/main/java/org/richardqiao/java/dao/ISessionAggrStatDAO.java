package org.richardqiao.java.dao;

import org.richardqiao.java.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 * 
 * @author richardqiao
 *
 */
public interface ISessionAggrStatDAO {

  /**
   * 插入session聚合统计结果
   * 
   * @param sessionAggrStat
   */
  void insert(SessionAggrStat sessionAggrStat);

}
