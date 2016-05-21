package com.highperformancespark.examples.objects;

import java.io.Serializable;

public class JavaCoffeeShop implements Serializable {
  private String zip;
  private String name;

  public JavaCoffeeShop(String zip, String name) {
    this.zip = zip;
    this.name = name;
  }

  public String getZip() {
    return zip;
  }

  public void setZip(String zip) {
    this.zip = zip;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}