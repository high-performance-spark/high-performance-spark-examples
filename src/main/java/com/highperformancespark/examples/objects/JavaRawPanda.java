package com.highperformancespark.examples.objects;

import java.io.Serializable;
import java.util.List;

public class JavaRawPanda implements Serializable {
  private long id;
  private String zip;
  private String pt;
  private boolean happy;
  private List<Double> attributes;

  /**
   * @param id panda id
   * @param zip zip code of panda residence
   * @param pt Type of panda as a string
   * @param happy if panda is happy
   * @param attributes array of panada attributes
   */
  public JavaRawPanda(long id, String zip, String pt, boolean happy, List<Double> attributes) {
    this.attributes = attributes;
    this.id = id;
    this.zip = zip;
    this.pt = pt;
    this.happy = happy;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getZip() {
    return zip;
  }

  public void setZip(String zip) {
    this.zip = zip;
  }

  public String getPt() {
    return pt;
  }

  public void setPt(String pt) {
    this.pt = pt;
  }

  public boolean isHappy() {
    return happy;
  }

  public void setHappy(boolean happy) {
    this.happy = happy;
  }

  public List<Double> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<Double> attributes) {
    this.attributes = attributes;
  }
}