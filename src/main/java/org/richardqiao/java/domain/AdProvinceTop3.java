package org.richardqiao.java.domain;

/**
 * 各省top3热门广告
 * 
 * @author richardqiao
 *
 */
public class AdProvinceTop3 {

  private String date;
  private String province;
  private long adid;
  private long clickCount;

  public AdProvinceTop3() {
    // TODO Auto-generated constructor stub
  }

  public AdProvinceTop3(String date, String province, long adid, long clickCount) {
    super();
    this.date = date;
    this.province = province;
    this.adid = adid;
    this.clickCount = clickCount;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getProvince() {
    return province;
  }

  public void setProvince(String province) {
    this.province = province;
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
