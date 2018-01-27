package org.richardqiao.java.dao;

import java.util.List;

import org.richardqiao.java.domain.AdProvinceTop3;

/**
 * 各省份top3热门广告DAO接口
 * 
 * @author richardqiao
 *
 */
public interface IAdProvinceTop3DAO {

  void updateBatch(List<AdProvinceTop3> adProvinceTop3s);

}
