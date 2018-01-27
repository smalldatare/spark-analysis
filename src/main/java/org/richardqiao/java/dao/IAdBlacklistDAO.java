package org.richardqiao.java.dao;

import java.util.List;
import java.util.Set;

import org.richardqiao.java.domain.AdBlacklist;

/**
 * 广告黑名单DAO接口
 * 
 * @author richardqiao
 *
 */
public interface IAdBlacklistDAO {

  /**
   * 批量插入广告黑名单用户
   * 
   * @param adBlacklists
   */
  void insertBatch(List<AdBlacklist> adBlacklists);

  /**
   * 查询所有广告黑名单用户
   * 
   * @return
   */
  Set<AdBlacklist> findAll();

}
