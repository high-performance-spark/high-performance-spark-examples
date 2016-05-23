package com.highperformancespark.examples.objects;

import java.io.Serializable;

public class JavaPandas implements Serializable {
  private String name;
  private String zip;
  private int pandaSize;
  private int age;

  /**
   * @param name      name of panda
   * @param zip       zip code
   * @param pandaSize size of panda in KG
   * @param age       age of panda
   */
  public JavaPandas(String name, String zip, int pandaSize, int age) {
    this.name = name;
    this.zip = zip;
    this.pandaSize = pandaSize;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getZip() {
    return zip;
  }

  public void setZip(String zip) {
    this.zip = zip;
  }

  public int getPandaSize() {
    return pandaSize;
  }

  public void setPandaSize(int pandaSize) {
    this.pandaSize = pandaSize;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

}
