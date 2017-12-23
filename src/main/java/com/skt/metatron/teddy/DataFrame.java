package com.skt.metatron.teddy;

import org.apache.commons.collections.ArrayStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataFrame implements Serializable {
  private static Logger LOGGER = LoggerFactory.getLogger(DataFrame.class);

  public static enum Type {
    UNKNOWN("unknown"),
    DOUBLE("double"),
    FLOAT("float"),
    INT64("long"),
    INT32("integer"),
    BOOLEAN("boolean"),
    STRING("string"),
    ARRAY("array"),
    MAP("map"),
    STRUCT("struct"),
    DATE("date");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  private int colCnt;
  private List<String> colNames;
  private List<Type> colTypes;
  private List<List<Object>> objGrid;

  public DataFrame(List<String> colNames) {
    this.colCnt = colNames.size();
    this.colNames = colNames;
    this.colTypes = new ArrayList<>();

    for (int c = 0; c < colCnt; c++) {
      this.colTypes.add(Type.STRING);
    }
  }

  public DataFrame() {
    colCnt = 0;
    colNames = new ArrayList<>();
    colTypes = new ArrayList<>();
    objGrid = new ArrayList();
  }

  public void setGrid(List<String[]> strGrid) {
    if (strGrid == null) {
      LOGGER.warn("setGrid(): null grid");
      return;
    }

    if (strGrid.size() == 0) {
      LOGGER.warn("setGrid(): empty grid");
      return;
    }

    if (colCnt == 0) {
      colCnt = strGrid.get(0).length;

      for (int c = 1; c <= colCnt; c++) {
        colNames.add("column" + c);
        colTypes.add(Type.STRING);
      }
    }

    for (String[] strRow : strGrid) {
      List<Object> objRow = new ArrayList();
      for (int c = 0; c < colCnt; c++) {
        objRow.add(strRow[c]);
      }
      objGrid.add(objRow);
    }
  }

  public void show() {
    show(20);
  }

  public void show(int limit) {
    limit = objGrid.size() < limit ? objGrid.size() : limit;
    List<Integer> widths = new ArrayList<>();
    for (int c = 0; c < colNames.size(); c++) {
      widths.add(colNames.get(c).length());
    }
    for (int r = 0; r < limit; r++) {
      List<Object> objRow = objGrid.get(r);
      for (int c = 0; c < objRow.size(); c++) {
        int colLen = objRow.get(c).toString().length();
        if (colLen > widths.get(c)) {
          widths.set(c, colLen);
        }
      }
    }

    showSep(widths);
    showColNames(widths);
    showSep(widths);
    for (int r = 0; r < objGrid.size(); r++) {
      showRow(widths, objGrid.get(r));
    }
    showSep(widths);
  }

  private void showSep(List<Integer> withes) {
    System.out.print("+");
    for (int width : withes) {
      for (int i = 0; i < width; i++) {
        System.out.print("-");
      }
      System.out.print("+");
    }
    System.out.println("");
  }

  private void showColNames(List<Integer> widths) {
    System.out.print("|");
    for (int i = 0; i < colCnt; i++) {
      System.out.print(String.format("%" + widths.get(i) + "s", colNames.get(i)));
      System.out.print("|");
    }
    System.out.println("");
  }

  private void showRow(List<Integer> widths, List<Object> objRow) {
    System.out.print("|");
    for (int i = 0; i < colCnt; i++) {
      System.out.print(String.format("%" + widths.get(i) + "s", objRow.get(i).toString()));
      System.out.print("|");
    }
    System.out.println("");
  }

  private DataFrame clone(DataFrame df) {
    return (DataFrame) org.apache.commons.lang.SerializationUtils.clone(this);
  }

  private void renameInPlace(String existingName, String newName) {
    for (int c = 0; c < colCnt; c++) {
      if (colNames.get(c).equals(existingName)) {
        colNames.set(c, newName);
        break;
      }
    }
  }

  public DataFrame select(List<String> targetColNames) {
    return project(targetColNames, true);
  }

  public DataFrame drop(List<String> targetColNames) {
    return project(targetColNames, false);
  }

  public DataFrame project(List<String> targetColNames, boolean select) {
    List<Integer> selectedColNos = new ArrayList<>();
    for (int i = 0; i < colCnt; i++) {
      if (select) {
        if (targetColNames.contains(colNames.get(i)) == true) {
          selectedColNos.add(i);
        }
      } else {
        if (targetColNames.contains(colNames.get(i)) == false) {
          selectedColNos.add(i);
        }
      }
    }

    DataFrame df = new DataFrame();
    df.colCnt = selectedColNos.size();
    for (int colno : selectedColNos) {
      df.colNames.add(this.colNames.get(colno));
      df.colTypes.add(this.colTypes.get(colno));
    }

    for (List<Object> row : this.objGrid) {
      List<Object> newRow = new ArrayList<>();
      for (int colno : selectedColNos) {
        newRow.add(row.get(colno));
      }
      df.objGrid.add(newRow);
    }
    return df;
  }

  public DataFrame withColumnRenamed(String existingName, String newName) {
    DataFrame df = clone(this);
    df.renameInPlace(existingName, newName);
    return df;
  }

}
