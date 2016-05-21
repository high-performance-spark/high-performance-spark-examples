package com.highperformancespark.examples.objects;

import java.io.Serializable;
import java.util.List;

public class JavaPandaPlace implements Serializable {
  private String name;
  private List<JavaRawPanda> pandas;

  /**
   * @param name place name
   * @param pandas pandas in that place
   */
  public JavaPandaPlace(String name, List<JavaRawPanda> pandas) {
    this.name = name;
    this.pandas = pandas;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<JavaRawPanda> getPandas() {
    return pandas;
  }

  public void setPandas(List<JavaRawPanda> pandas) {
    this.pandas = pandas;
  }
}