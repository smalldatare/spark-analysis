package org.richardqiao.java.dao;

import org.richardqiao.java.domain.Task;

/**
 * 任务管理DAO接口
 * 
 * @author richardqiao
 *
 */
public interface ITaskDAO {

  /**
   * 根据主键查询任务
   * 
   * @param taskid
   *            主键
   * @return 任务
   */
  Task findById(long taskid);

}
