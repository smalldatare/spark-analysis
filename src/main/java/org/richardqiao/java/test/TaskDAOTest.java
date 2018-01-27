package org.richardqiao.java.test;

import org.richardqiao.java.dao.ITaskDAO;
import org.richardqiao.java.dao.factory.DAOFactory;
import org.richardqiao.java.domain.Task;

/**
 * 任务管理DAO测试类
 * 
 * @author richardqiao
 *
 */
public class TaskDAOTest {

  public static void main(String[] args) {
    ITaskDAO taskDAO = DAOFactory.getTaskDAO();
    Task task = taskDAO.findById(2);
    System.out.println(task.getTaskName());
  }

}
