package com.skt.metatron.teddy;

import com.skt.metatron.discovery.common.preparation.rule.expr.Expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row implements Expr.NumericBinding {
  List<Object> objCols;
  Map<String, Integer> nameIdxs;
  int colCnt;

  List<Integer> cmpKeyIdxs;
  List<DataFrame.TYPE> cmpKeyTypes;

  public Row() {
    objCols = new ArrayList<>();
    nameIdxs = new HashMap<>();
    colCnt = 0;
  }

  public void add(String colName, Object objCol) {
    objCols.add(objCol);
    nameIdxs.put(colName, colCnt++);
  }

  public int size() {
    return colCnt;
  }

  @Override
  public Object get(String colName) {
    return objCols.get(nameIdxs.get(colName));
  }

  public Object get(int colNo) {
    return objCols.get(colNo);
  }
}
