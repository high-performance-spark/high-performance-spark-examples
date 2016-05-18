package com.highperformancespark.examples.objects;

import com.highperformancespark.examples.dataframe.RawPanda;

public class JavaPandaPlace {
  private String name;
  private RawPanda[] pandas;

  /**
   * @param name place name
   * @param pandas pandas in that place
   */
  public JavaPandaPlace(String name, RawPanda[] pandas) {
    this.name = name;
    this.pandas = pandas;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public RawPanda[] getPandas() {
    return pandas;
  }

  public void setPandas(RawPanda[] pandas) {
    this.pandas = pandas;
  }
}