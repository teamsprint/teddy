package com.skt.metatron.teddy;

import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.discovery.common.preparation.rule.expr.*;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataFrame implements Serializable {
  private static Logger LOGGER = LoggerFactory.getLogger(DataFrame.class);

  private int colCnt;
  private List<String> colNames;
  private List<ExprType> colTypes;
  private List<Row> objGrid;

  public DataFrame(List<String> colNames) {
    this.colCnt = colNames.size();
    this.colNames = colNames;
    this.colTypes = new ArrayList<>();

    for (int colno = 0; colno < colCnt; colno++) {
      this.colTypes.add(ExprType.STRING);
    }
  }

  public DataFrame() {
    colCnt = 0;
    colNames = new ArrayList<>();
    colTypes = new ArrayList<>();
    objGrid = new ArrayList<>();
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

      for (int colno = 1; colno <= colCnt; colno++) {
        colNames.add("column" + colno);
        colTypes.add(ExprType.STRING);
      }
    }

    for (String[] strRow : strGrid) {
      Row row = new Row();
      for (int colno = 0; colno < colCnt; colno++) {
        row.add(colNames.get(colno), strRow[colno]);
      }
      objGrid.add(row);
    }
  }

  public void show() {
    show(20);
  }

  public void show(int limit) {
    limit = objGrid.size() < limit ? objGrid.size() : limit;
    List<Integer> widths = new ArrayList<>();
    for (int colno = 0; colno < colNames.size(); colno++) {
      widths.add(Math.max(colNames.get(colno).length(), colTypes.get(colno).toString().length()));
    }
    for (int rowno = 0; rowno < limit; rowno++) {
      Row row = objGrid.get(rowno);
      for (int colno = 0; colno < row.size(); colno++) {
        Object objCol = row.get(colNames.get(colno));
        int colLen = objCol.toString().length();
        if (colLen > widths.get(colno)) {
          widths.set(colno, colLen);
        }
      }
    }

    showSep(widths);
    showColNames(widths);
    showColTypes(widths);
    showSep(widths);
    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      showRow(widths, objGrid.get(rowno));
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

  private void showColTypes(List<Integer> widths) {
    System.out.print("|");
    for (int i = 0; i < colCnt; i++) {
      System.out.print(String.format("%" + widths.get(i) + "s", colTypes.get(i)));
      System.out.print("|");
    }
    System.out.println("");
  }

  private void showRow(List<Integer> widths, Row row) {
    System.out.print("|");
    for (int i = 0; i < colCnt; i++) {
      System.out.print(String.format("%" + widths.get(i) + "s", row.get(i).toString()));
      System.out.print("|");
    }
    System.out.println("");
  }

  private DataFrame clone(DataFrame df) {
    return (DataFrame) org.apache.commons.lang.SerializationUtils.clone(this);
  }

  private void renameInPlace(String existingName, String newName) {
    for (int colno = 0; colno < colCnt; colno++) {
      if (colNames.get(colno).equals(existingName)) {
        colNames.set(colno, newName);
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
    DataFrame newDf = new DataFrame();

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

    newDf.colCnt = selectedColNos.size();
    for (int colno : selectedColNos) {
      newDf.colNames.add(this.colNames.get(colno));
      newDf.colTypes.add(this.colTypes.get(colno));
    }

    for (Row row : this.objGrid) {
      Row newRow = new Row();
      for (int colno : selectedColNos) {
        newRow.add(colNames.get(colno), row.get(colno));
      }
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public DataFrame doRename(Rename rename) {
    DataFrame newDf = new DataFrame();
    String fromColName = rename.getCol();
    String toColName = rename.getTo();
    int targetColNo = -1;

    newDf.colCnt = colCnt;
    for (int colno = 0; colno < colCnt; colno++) {
      String colName = colNames.get(colno);
      if (targetColNo == -1 && colName.equals(fromColName)) {
        newDf.colNames.add(toColName);
        targetColNo = colno;
      } else {
        newDf.colNames.add(colName);
      }
      newDf.colTypes.add(colTypes.get(colno));
    }

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (int colno = 0; colno < colCnt; colno++) {
        if (colno == targetColNo) {
          newRow.add(toColName, row.get(colno));
        } else {
          newRow.add(colNames.get(colno), row.get(colno));
        }
      }
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public String parseRuleString(String ruleString) throws IOException {
    Rule rule = new RuleVisitorParser().parse(ruleString);
    ObjectMapper mapper = new ObjectMapper();
    String json = null;
    try {
      json = mapper.writeValueAsString(rule);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return json;
  }

  private ExprType decideType(Expression expr) throws TeddyException {
    ExprType resultType = null;
    String errmsg;
    int i;

    // Identifier
    if (expr instanceof Identifier.IdentifierExpr) {
      String colName = ((Identifier.IdentifierExpr) expr).getValue();
      for (i = 0; i < colCnt; i++) {
        if (colNames.get(i).equals(colName)) {
          resultType = colTypes.get(i);
          break;
        }
      }
      if (i == colCnt) {
        throw new TeddyException("decideType(): colname not found: " + colName);
      }
    }
    // Constant
    else if (expr instanceof Constant) {
      if (expr instanceof Constant.StringExpr) {
        resultType = ExprType.STRING;
      } else if (expr instanceof Constant.LongExpr) {
        resultType = ExprType.LONG;
      } else if (expr instanceof Constant.DoubleExpr) {
        resultType = ExprType.DOUBLE;
      } else {
        errmsg = String.format("decideType(): unsupported constant type: expr=%s", expr);   // TODO: boolean, array support
        throw new TeddyException(errmsg);
      }
    }
    // Binary Operation
    else if (expr instanceof Expr.BinaryNumericOpExprBase) {
      ExprType left = decideType(((Expr.BinaryNumericOpExprBase) expr).getLeft());
      ExprType right = decideType(((Expr.BinaryNumericOpExprBase) expr).getRight());
      if (left == right) {
        return left;
      }
      String msg = String.format("decideType(): type mismatch: left=%s right=%s expr=%s", left, right, expr);
      throw new TeddyException(msg);  // FIXME: expr이 %s로 찍히는지 체크
    }
    System.out.println(String.format("decideType(): resultType=%s expr=%s", resultType, expr));
    return resultType;
  }

  private Object eval(int rowno, Expression expr) throws TeddyException {
    ExprType resultType = null;
    Object resultObj = null;
    String errmsg;
    int colno;

    // Identifier
    if (expr instanceof Identifier.IdentifierExpr) {
      String colName = ((Identifier.IdentifierExpr) expr).getValue();
      for (colno = 0; colno < colCnt; colno++) {
        if (colNames.get(colno).equals(colName)) {
          resultType = colTypes.get(colno);
          resultObj = objGrid.get(rowno).get(colno);
          break;
        }
      }
      if (colno == colCnt) {
        throw new TeddyException("column not found: " + colName);
      }
    }
    // Constant
    else if (expr instanceof Constant) {
      if (expr instanceof Constant.StringExpr) {
        resultType = ExprType.STRING;
      } else if (expr instanceof Constant.LongExpr) {
        resultType = ExprType.LONG;
      } else if (expr instanceof Constant.DoubleExpr) {
        resultType = ExprType.DOUBLE;
      } else {
        errmsg = String.format("eval(): unsupported constant type: expr=%s", expr);   // TODO: boolean, array support
        throw new TeddyException(errmsg);
      }
      resultObj = ((Constant)expr).getValue();
    }
    // Binary Operation
    else if (expr instanceof Expr.BinaryNumericOpExprBase) {
      ExprEval result = ((Expr.BinaryNumericOpExprBase) expr).eval(objGrid.get(rowno));
//      ExprEval left = ((Expr.BinaryNumericOpExprBase) expr).getLeft().eval(objGrid.get(rowno));
//      ExprEval right = ((Expr.BinaryNumericOpExprBase) expr).getRight().eval(null);
//      if (left.rhs == right.rhs) {
//        if (left.rhs == ExprType.LONG) {
//          resultObj = ((Expr.BinaryNumericOpExprBase) expr)
//        } else if (left.rhs == ExprType.DOUBLE) {
//          resultObj = ((Expr.BinaryNumericOpExprBase) expr).getOp().eval(null);
//        }
//      } else {
//        String msg = String.format("eval(): type mismatch: left=%s right=%s expr=%s", left.rhs, right.rhs, expr);
//        throw new TeddyException(msg);  // FIXME: expr이 %s로 찍히는지 체크
//      }

      resultObj = result.value();
    }
    System.out.println(String.format("eval(): resultType=%s resultObj=%s expr=%s", resultType, resultObj.toString(), expr));
    return resultObj;
  }

  private Object cast(Object obj, ExprType fromType, ExprType toType) {
    switch (toType) {
      case DOUBLE:
        switch (fromType) {
          case DOUBLE:
            return obj;
          case LONG:
            return Double.valueOf(((Long)obj).doubleValue());
          case STRING:
            return Double.valueOf(obj.toString());
        }

      case LONG:
        switch (fromType) {
          case DOUBLE:
            return Long.valueOf(((Double)obj).longValue());
          case LONG:
            return obj;
          case STRING:
            return Long.valueOf(obj.toString());
        }

      case STRING:
        break;
    }
    return obj.toString();
  }

  public DataFrame doSetType(SetType setType) throws TeddyException {
    DataFrame newDf = new DataFrame();
    String targetColName = setType.getCol();
    ExprType toType = ExprType.bestEffortOf(setType.getType());
    int targetColNo = -1;

    newDf.colCnt = colCnt;
    for (int colno = 0; colno < colCnt; colno++) {
      String colName = colNames.get(colno);
      newDf.colNames.add(colName);
      if (targetColNo == -1 && colName.equals(targetColName)) {
        newDf.colTypes.add(toType);
        targetColNo = colno;
      } else {
        newDf.colTypes.add(colTypes.get(colno));
      }
    }

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (int colno = 0; colno < colCnt; colno++) {
        newRow.add(colNames.get(colno), colno == targetColNo ? cast(row.get(colno), colTypes.get(colno), toType) : row.get(colno));
      }
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public DataFrame doSetInternal(String targetColName, Expression expr) throws TeddyException {
    DataFrame newDf = new DataFrame();
    int targetColNo = -1;

    for (int colno = 0; colno < colCnt; colno++) {
      String colName = colNames.get(colno);
      newDf.colNames.add(colName);
      if (targetColNo == -1 && colName.equals(targetColName)) {
        newDf.colTypes.add(decideType(expr));
        targetColNo = colno;
      } else {
        newDf.colTypes.add(colTypes.get(colno));
      }
    }
    if (targetColNo == -1) {            // targetColName이 존재하지 않음 --> derive()
      newDf.colCnt = colCnt + 1;
      newDf.colNames.add(targetColName);
      newDf.colTypes.add(decideType(expr));
      targetColNo = newDf.colCnt - 1;   // put new expr at the end
    } else {
      newDf.colCnt = colCnt;
    }

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (int colno = 0; colno < newDf.colCnt; colno++) {
        if (colno == targetColNo) {
          newRow.add(targetColName, eval(rowno, expr));
        } else {
          newRow.add(colNames.get(colno), row.get(colno));
        }
      }
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public DataFrame doSet(Set set) throws TeddyException {
    return doSetInternal(set.getCol(), set.getValue());
  }

  public DataFrame doDerive(Derive derive) throws TeddyException {
    String targetColName = derive.getAs();

    // 기존 column 이름과 겹치면 안됨.
    for (int colno = 0; colno < colCnt; colno++) {
      if (colNames.get(colno).equalsIgnoreCase(targetColName)) {
        throw new TeddyException("doDerive(): colname not exists: " + targetColName);
      }
    }

    return doSetInternal(derive.getAs(), derive.getValue());
  }
}
