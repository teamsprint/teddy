package com.skt.metatron.teddy;

import com.skt.metatron.discovery.common.preparation.rule.expr.Expr;
import com.skt.metatron.teddy.DataFrame.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row implements Expr.NumericBinding {
  List<Object> objCols;
  Map<String, Integer> nameIdxs;
  int colCnt;

  List<Integer> cmpKeyIdxs;
  List<DataType> cmpKeyTypes;

  public Row() {
    objCols = new ArrayList<>();
    nameIdxs = new HashMap<>();
    colCnt = 0;
  }

  public void add(String colName, Object objCol) {
    objCols.add(objCol);
    nameIdxs.put(colName, colCnt++);
  }

  public void set(String colName, Object objCol) {
    assert nameIdxs.containsKey(colName) : colName;
    objCols.set(nameIdxs.get(colName), objCol);
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
