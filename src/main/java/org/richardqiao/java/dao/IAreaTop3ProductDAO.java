package org.richardqiao.java.dao;

import java.util.List;

import org.richardqiao.java.domain.AreaTop3Product;

/**
 * 各区域top3热门商品DAO接口
 * 
 * @author richardqiao
 *
 */
public interface IAreaTop3ProductDAO {

  void insertBatch(List<AreaTop3Product> areaTopsProducts);

}
