package org.richardqiao.java.domain;

import org.richardqiao.java.util.DateUtils;

/**
 * 用户广告点击量
 * 
 * @author richardqiao
 *
 */
public class AdUserClickCount {

  private String date;
  private long userid;
  private long adid;
  private long clickCount;

  public AdUserClickCount(String key, long clickCount) {
    String[] keySplited = key.split("_");
    this.date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
    this.userid = Long.valueOf(keySplited[1]);
    this.adid = Long.valueOf(keySplited[2]);
    this.clickCount = clickCount;
  }

  public AdUserClickCount(String date, long userid, long adid, long clickCount) {
    super();
    this.date = date;
    this.userid = userid;
    this.adid = adid;
    this.clickCount = clickCount;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public long getUserid() {
    return userid;
  }

  public void setUserid(long userid) {
    this.userid = userid;
  }

  public long getAdid() {
    return adid;
  }

  public void setAdid(long adid) {
    this.adid = adid;
  }

  public long getClickCount() {
    return clickCount;
  }

  public void setClickCount(long clickCount) {
    this.clickCount = clickCount;
  }

}
