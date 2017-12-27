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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFrame implements Serializable {
  private static Logger LOGGER = LoggerFactory.getLogger(DataFrame.class);

  enum TYPE {
    DOUBLE,
    LONG,
    STRING,
    ARRAY,
    MAP,
    INVALID
  }

  private static TYPE getType(ExprType exprType) {
    switch (exprType) {
      case DOUBLE:
        return TYPE.DOUBLE;
      case LONG:
        return TYPE.LONG;
      case STRING:
        return TYPE.STRING;
    }
    assert false : exprType;
    return TYPE.INVALID;
  }

  private int colCnt;
  private List<String> colNames;
  private List<TYPE> colTypes;
  private List<Row> objGrid;

  // 처음 data를 가져오는 정보부터, 현재 dataframe에 이르기까지의 모든 정보
  // 내 스스로는 필요없음. upstreamDataFrame의 내용이 필요할 때가 있음 (join, union, wrangled -> wrangled)
  private List<DataFrame> upstreamDataFrame;
  private String dsType;
  private String importType;
  private String filePath;
  private String queryStmt;
  private List<String> ruleStrings;

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
        colTypes.add(TYPE.STRING);
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
    for (int rowno = 0; rowno < limit; rowno++) {
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

  public DataFrame doDrop(Drop drop) throws TeddyException {
    List<String> targetColNames = new ArrayList<>();

    Expr expr = (Expr) drop.getCol();
    if (expr instanceof Identifier.IdentifierExpr) {
      targetColNames.add(((Identifier.IdentifierExpr) expr).getValue());
    } else if (expr instanceof Identifier.IdentifierArrayExpr) {
      targetColNames.addAll(((Identifier.IdentifierArrayExpr) expr).getValue());
    } else {
      assert false : expr;
    }

    for (String colName : targetColNames) {
      if (!colNames.contains(colName)) {
        throw new TeddyException("doDrop(): column not found: " + colName);
      }
    }

    return drop(targetColNames);
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

  private TYPE decideType(Expression expr) throws TeddyException {
    TYPE resultType = TYPE.INVALID;
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
        resultType = getType(ExprType.STRING);
      } else if (expr instanceof Constant.LongExpr) {
        resultType = getType(ExprType.LONG);
      } else if (expr instanceof Constant.DoubleExpr) {
        resultType = getType(ExprType.DOUBLE);
      } else {
        errmsg = String.format("decideType(): unsupported constant type: expr=%s", expr);   // TODO: boolean, array support
        throw new TeddyException(errmsg);
      }
    }
    // Binary Operation
    else if (expr instanceof Expr.BinaryNumericOpExprBase) {
      TYPE left = decideType(((Expr.BinaryNumericOpExprBase) expr).getLeft());
      TYPE right = decideType(((Expr.BinaryNumericOpExprBase) expr).getRight());
      if (left == right) {
        return left;
      }
      String msg = String.format("decideType(): type mismatch: left=%s right=%s expr=%s", left, right, expr);
      throw new TeddyException(msg);
    }
    System.out.println(String.format("decideType(): resultType=%s expr=%s", resultType, expr));
    return resultType;
  }

  private Object eval(int rowno, Expression expr) throws TeddyException {
    TYPE resultType = TYPE.INVALID;
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
        throw new TeddyException("eval(): column not found: " + colName);
      }
    }
    // Constant
    else if (expr instanceof Constant) {
      if (expr instanceof Constant.StringExpr) {
        resultType = getType(ExprType.STRING);
      } else if (expr instanceof Constant.LongExpr) {
        resultType = getType(ExprType.LONG);
      } else if (expr instanceof Constant.DoubleExpr) {
        resultType = getType(ExprType.DOUBLE);
      } else {
        errmsg = String.format("eval(): unsupported constant type: expr=%s", expr);   // TODO: boolean, array support
        throw new TeddyException(errmsg);
      }
      resultObj = ((Constant)expr).getValue();
    }
    // Binary Operation
    else if (expr instanceof Expr.BinaryNumericOpExprBase) {
      ExprEval result = ((Expr.BinaryNumericOpExprBase) expr).eval(objGrid.get(rowno));
      resultObj = result.value();
    }
    System.out.println(String.format("eval(): resultType=%s resultObj=%s expr=%s", resultType, resultObj.toString(), expr));
    return resultObj;
  }

  private Object cast(Object obj, TYPE fromType, TYPE toType) throws TeddyException {
    switch (toType) {
      case DOUBLE:
        switch (fromType) {
          case DOUBLE:
            return obj;
          case LONG:
            return Double.valueOf(((Long)obj).doubleValue());
          case STRING:
            return Double.valueOf(obj.toString());
          default:
            throw new TeddyException("cast(): cannot cast to " + toType);
        }

      case LONG:
        switch (fromType) {
          case DOUBLE:
            return Long.valueOf(((Double)obj).longValue());
          case LONG:
            return obj;
          case STRING:
            return Long.valueOf(obj.toString());
          default:
            throw new TeddyException("cast(): cannot cast to " + toType);
        }

      case STRING:
        break;

      default:
        throw new TeddyException("cast(): cannot cast from " + toType);
    }
    return obj.toString();
  }

  public DataFrame doSetType(SetType setType) throws TeddyException {
    DataFrame newDf = new DataFrame();
    String targetColName = setType.getCol();
    TYPE toType = getType(ExprType.bestEffortOf(setType.getType()));
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
    if (targetColNo == -1) {
      throw new TeddyException("doSetType(): column not found");
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
        throw new TeddyException("doDerive(): colname exists: " + targetColName);
      }
    }

    return doSetInternal(derive.getAs(), derive.getValue());
  }

  public DataFrame doHeader(Header header) throws TeddyException {
    DataFrame newDf = new DataFrame();
    int targetRowno = header.getRownum().intValue() - 1;
    if (targetRowno < 0) {
      throw new TeddyException("doHeader(): rownum should be >= 1: rownum=" + (targetRowno + 1));
    }

    newDf.colCnt = colCnt;

    Row targetRow = objGrid.get(targetRowno);
    for (int colno = 0; colno < colCnt; colno++) {
      newDf.colNames.add(colno, (String)targetRow.get(colno));
      newDf.colTypes.add(colTypes.get(colno));
    }

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      if (rowno != targetRowno) {
        Row row = objGrid.get(rowno);
        Row newRow = new Row();
        for (int colno = 0; colno < colCnt; colno++) {
          newRow.add(newDf.colNames.get(colno), row.get(colno));
        }
        newDf.objGrid.add(newRow);
      }
    }

    return newDf;
  }

  public void addJoinedRow(Row lrow, List<String> leftSelectColNames, Row rrow, List<String> rightSelectColNames) {
    Row newRow = new Row();
    for (String colName : leftSelectColNames) {
    newRow.add(colName, lrow.get(colName));                             // left에서 온 컬럼은 이름 그대로 넣음
    }
    for (String colName : rightSelectColNames) {
      newRow.add(this.colNames.get(newRow.colCnt), rrow.get(colName));  // 필요한 경우 "r_"이 붙은 컬럼 이름
    }
    objGrid.add(newRow);
  }

  public DataFrame join(DataFrame rightDataFrame, List<String> leftSelectColNames, List<String> rightSelectColNames,
                        String condition, String joinType, int limitRowCnt) throws TeddyException {
    String fakeRuleString = "keep row: " + condition;
    Rule rule = new RuleVisitorParser().parse(fakeRuleString);
    Expr.BinAsExpr predicate = (Expr.BinAsExpr)((Keep)rule).getRow();
    if (!predicate.getOp().equals("=")) {
      throw new TeddyException("join(): join type not suppoerted: op: " + predicate.getOp());
    }
    Expr.BinEqExpr eqExpr = new Expr.BinEqExpr(predicate.getOp(), predicate.getLeft(), predicate.getRight());
    boolean typeChecked = false;

    DataFrame newDf = new DataFrame();
    newDf.colCnt = leftSelectColNames.size() + rightSelectColNames.size();
    for (String colName : leftSelectColNames) {
      newDf.colNames.add(colName);
      newDf.colTypes.add(colTypes.get(colNames.indexOf(colName)));
    }
    for (String colName : rightSelectColNames) {
      if (leftSelectColNames.contains(colName)) {
        newDf.colNames.add("r_" + colName);       // 같은 column이름이 있을 경우 right에서 온 것에 "r_"을 붙여준다. (twinkle과 동일한 규칙)
      } else {
        newDf.colNames.add(colName);
      }
      newDf.colTypes.add(rightDataFrame.colTypes.get(rightDataFrame.colNames.indexOf(colName)));
    }

    for (int lrowno = 0; lrowno < objGrid.size(); lrowno++) {
      Row lrow = objGrid.get(lrowno);

      for (int rrowno = 0; rrowno < rightDataFrame.objGrid.size(); rrowno++) {
        Row rrow = rightDataFrame.objGrid.get(rrowno);

        Identifier.IdentifierExpr left = (Identifier.IdentifierExpr) eqExpr.getLeft();
        Identifier.IdentifierExpr right = (Identifier.IdentifierExpr) eqExpr.getRight();
        ExprType ltype = left.eval(lrow).type();

        if (!typeChecked) {
          ExprType rtype = right.eval(rrow).type();
          if (ltype != rtype) {
            throw new TeddyException(String.format("join(): predicate type mismatch: left=%s right%s", ltype.toString(), rtype.toString()));
          }
          typeChecked = true;
        }

        // lrow와 rrow를 합쳐서 NumericBindings를 만들지도 않았고, 그런다 해도 a=a 와 같은 predicate은 어쩌할 도리가 없기 때문에, 각각 eval을 한다.
        // type 구분을 해서 eval을 해야한다는 귀찮음이 있다.
        switch (ltype) {
          case DOUBLE:
            if (left.eval(lrow).asDouble() == right.eval(rrow).asDouble()) {
              newDf.addJoinedRow(lrow, leftSelectColNames, rrow, rightSelectColNames);
            }
            break;
          case LONG:
            if (left.eval(lrow).asLong() == right.eval(rrow).asLong()) {
              newDf.addJoinedRow(lrow, leftSelectColNames, rrow, rightSelectColNames);
            }
          case STRING:
            if (left.eval(lrow).asString().equals(right.eval(rrow).asString())) {
              newDf.addJoinedRow(lrow, leftSelectColNames, rrow, rightSelectColNames);
            }
        }
        if (newDf.objGrid.size() == limitRowCnt) {
          return newDf;
        }
      } // end of each rrow
    }
    return newDf;
  }

  public DataFrame union(List<DataFrame> slaveDataFrames, int limitRowCnt) throws TeddyException {
    DataFrame newDf = new DataFrame();

    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    // master도 추가
    slaveDataFrames.add(0, this);
    for (DataFrame df : slaveDataFrames) {
      for (Row row : df.objGrid) {
        if (newDf.objGrid.size() >= limitRowCnt) {
          return newDf;
        }

        newDf.objGrid.add(row);
      }
    }
    return newDf;
  }

  public DataFrame doExtract(Extract extract) throws TeddyException {
    String targetColName = extract.getCol();
    int targetColno = -1;
    Expression expr = extract.getOn();
    Expression quote = extract.getQuote();
    int limit = extract.getLimit();
    int rowno, colno;

    if (!colNames.contains(targetColName)) {
      throw new TeddyException("doExtract(): column not found: " + targetColName);
    } else if (limit <= 0) {
      throw new TeddyException("doExtract(): limit should be >= 0: " + limit);
    } else {
      for (colno = 0; colno < colCnt; colno++) {
        if (colNames.get(colno).equals(targetColName)) {
          if (colTypes.get(colno) != TYPE.STRING) {
            throw new TeddyException("doExtract(): works only on STRING: " + colTypes.get(colno));
          }
          targetColno = colno;
        }
      }
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    List<String> newColNames = new ArrayList<>();
    for (int i = 0; i < limit; i++) {
      String newColName = "extract_" + (i + 1);
      while (newDf.colNames.contains(newColName)) {
        newColName += "_1";
      }
      newColNames.add(newColName);  // for newRow add
      newDf.colNames.add(newColName);
      newDf.colTypes.add(TYPE.STRING);
      newDf.colCnt++;
    }

    String patternStr;
    if (expr instanceof Constant.StringExpr) {
      patternStr = ((Constant.StringExpr) expr).getEscapedValue();
    } else if (expr instanceof RegularExpr) {
      patternStr = ((RegularExpr) expr).getEscapedValue().replaceAll("[\\\\]+", "\\\\");
    } else {
      throw new TeddyException("doExtract(): illegal pattern type: " + expr.toString());
    }

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (String colName : colNames) {
        newRow.add(colName, row.get(colName));
      }
      String targetStr = (String)row.get(targetColno);
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(targetStr);
      for (int i = 0; i < limit; i++) {
        if (matcher.find()) {
          String newColData = targetStr.substring(matcher.start(), matcher.end());
          newRow.add(newColNames.get(i), newColData);
        } else {
          newRow.add(newColNames.get(i), "");
        }
      }
      newDf.objGrid.add(newRow);
    }

    return newDf;
  }

  public DataFrame doNest(Nest nest) throws TeddyException {
    Expression targetExpr = nest.getCol();
    List<String> targetColNames = new ArrayList<>();
    String into = nest.getInto();
    String as = nest.getAs();
    int rowno, colno;

    if (targetExpr instanceof Identifier.IdentifierExpr) {
      targetColNames.add(((Identifier.IdentifierExpr) targetExpr).getValue());
    } else if (targetExpr instanceof Identifier.IdentifierArrayExpr) {
      targetColNames.addAll(((Identifier.IdentifierArrayExpr) targetExpr).getValue());
    } else {
      assert false : targetExpr;
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    newDf.colCnt++;
    newDf.colNames.add(as);
    newDf.colTypes.add(into.equalsIgnoreCase("ARRAY") ? TYPE.ARRAY : TYPE.MAP);

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row  = objGrid.get(rowno);
      Row newRow = new Row();
      for (colno = 0; colno < colCnt; colno++) {
        newRow.add(colNames.get(colno), row.get(colno));
      }

      if (newDf.colTypes.get(newDf.colCnt - 1) == TYPE.ARRAY) {
        List<String> quotedValues = new ArrayList<>();
        for (String colName : targetColNames) {
          quotedValues.add("\"" + row.get(colName) + "\"");
        }
        newRow.add(as, "[" + String.join(",", quotedValues) + "]");
      } else {
        List<String> quotedKayVals = new ArrayList<>();
        for (String colName : targetColNames) {
          quotedKayVals.add("\"" + colName + "\":\"" + row.get(colName) + "\"");
        }
        newRow.add(as, "{" + String.join(",", quotedKayVals) + "}");
      }
      newDf.objGrid.add(newRow);
    }

    return newDf;
  }
}
