package com.highperformancespark.examples.objects;

import java.io.Serializable;

public class JavaPandaInfo implements Serializable {
  private String place;
  private String pandaType;
  private int happyPandas;
  private int totalPandas;

  /**
   * @param place       name of place
   * @param pandaType   type of pandas in this place
   * @param happyPandas number of happy pandas in this place
   * @param totalPandas total number of pandas in this place
   */
  public JavaPandaInfo(String place, String pandaType, int happyPandas, int totalPandas) {
    this.place = place;
    this.pandaType = pandaType;
    this.happyPandas = happyPandas;
    this.totalPandas = totalPandas;
  }

  public String getPlace() {
    return place;
  }

  public void setPlace(String place) {
    this.place = place;
  }

  public String getPandaType() {
    return pandaType;
  }

  public void setPandaType(String pandaType) {
    this.pandaType = pandaType;
  }

  public int getHappyPandas() {
    return happyPandas;
  }

  public void setHappyPandas(int happyPandas) {
    this.happyPandas = happyPandas;
  }

  public int getTotalPandas() {
    return totalPandas;
  }

  public void setTotalPandas(int totalPandas) {
    this.totalPandas = totalPandas;
  }

}
