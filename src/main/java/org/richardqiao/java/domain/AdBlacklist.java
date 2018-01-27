package org.richardqiao.java.domain;

/**
 * 广告黑名单
 * 
 * @author richardqiao
 *
 */
public class AdBlacklist {

  private long userid;

  public AdBlacklist() {
    // TODO Auto-generated constructor stub
  }

  public AdBlacklist(long userid) {
    super();
    this.userid = userid;
  }

  public long getUserid() {
    return userid;
  }

  public void setUserid(long userid) {
    this.userid = userid;
  }

}
