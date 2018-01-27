package org.richardqiao.java.dao;

import java.util.List;

import org.richardqiao.java.domain.AdStat;

/**
 * 广告实时统计DAO接口
 * 
 * @author richardqiao
 *
 */
public interface IAdStatDAO {

  void updateBatch(List<AdStat> adStats);

}
