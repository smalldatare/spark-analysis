package org.richardqiao.java.dao;

import java.util.List;

import org.richardqiao.java.domain.AdClickTrend;

/**
 * 广告点击趋势DAO接口
 * 
 * @author richardqiao
 *
 */
public interface IAdClickTrendDAO {

  void updateBatch(List<AdClickTrend> adClickTrends);

}
