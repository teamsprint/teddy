package com.skt.metatron.teddy;

import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.discovery.common.preparation.rule.Set;
import com.skt.metatron.discovery.common.preparation.rule.expr.*;
import org.apache.commons.collections.map.HashedMap;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
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
    BOOLEAN,
    INVALID
  }

  enum AggrType {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX
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

  private TYPE getTypeOfColumn(String colName) throws TeddyException {
    int i;
    for (i = 0; i < colNames.size(); i++) {
      if (colNames.get(i).equals(colName)) {
        return colTypes.get(i);
      }
    }
    throw new TeddyException("getTypeOfColumn(): column not found: " + colName);
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
    // Function Operation
    else if (expr instanceof Expr.FunctionExpr) {
      List<Expr> args = ((Expr.FunctionExpr) expr).getArgs();
      if (args.size() == 1) {
        resultType = TYPE.BOOLEAN;
      } else if (args.size() == 3) {
        if (args.get(1) instanceof Constant.StringExpr && args.get(2) instanceof Constant.StringExpr) {
          return TYPE.STRING;
        }
      } else {
        throw new TeddyException("decideType(): invalid function arguments: " + args.size());
      }
    }
//    LOGGER.debug(String.format("decideType(): resultType=%s expr=%s", resultType, expr));
    return resultType;
  }

  private Object eval(Expression expr, int rowno) throws TeddyException {
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
      ExprEval binOpEval = ((Expr.BinaryNumericOpExprBase) expr).eval(objGrid.get(rowno));
      resultType = getType(binOpEval.type());
      resultObj = binOpEval.value();
    }
    // Function Operation
    else if (expr instanceof Expr.FunctionExpr) {
      try {
        ExprEval funcEval = ((Expr.FunctionExpr) expr).eval(objGrid.get(rowno));
        resultType = getType(funcEval.type());
        resultObj = funcEval.value();
      } catch (AssertionError e) {
        String msg = "eval(): unhandled error in function expression";
        LOGGER.error(msg);
        throw new TeddyException(msg);
      }
    }

//    System.out.println(String.format("eval(): resultType=%s resultObj=%s expr=%s", resultType, resultObj.toString(), expr));
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
          newRow.add(targetColName, eval(expr, rowno));
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
      newDf.colNames.add(colno, (String)targetRow.get(colno));  // colno 필요?
      newDf.colTypes.add(colTypes.get(colno));
    }

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      if (rowno == targetRowno) {
        continue;
      }

      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (int colno = 0; colno < colCnt; colno++) {
        newRow.add(newDf.colNames.get(colno), row.get(colno));
      }
      newDf.objGrid.add(newRow);
    }

    return newDf;
  }

  public void addJoinedRow(Row lrow, List<String> leftSelectColNames, Row rrow, List<String> rightSelectColNames) {
    Row newRow = new Row();
    for (String colName : leftSelectColNames) {
    newRow.add(colName, lrow.get(colName));                             // left에서 온 컬럼은 이름 그대로 넣음
    }
    for (String colName : rightSelectColNames) {
      newRow.add(this.colNames.get(newRow.colCnt), rrow.get(colName));  // 필요한 경우 "r_"이 붙은 컬럼 이름 (여기까지 온 것은 이미 붙은 상황)
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
      newDf.colNames.add(checkRightColName(colName));  // 같은 column이름이 있을 경우 right에서 온 것에 "r_"을 붙여준다. (twinkle과 동일한 규칙)
      newDf.colTypes.add(rightDataFrame.colTypes.get(rightDataFrame.colNames.indexOf(colName)));
    }

    for (int lrowno = 0; lrowno < objGrid.size(); lrowno++) {
      Row lrow = objGrid.get(lrowno);

      for (int rrowno = 0; rrowno < rightDataFrame.objGrid.size(); rrowno++) {
        Row rrow = rightDataFrame.objGrid.get(rrowno);

        Identifier.IdentifierExpr left = (Identifier.IdentifierExpr) eqExpr.getLeft();    // loop 밖으로
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

  public DataFrame doCountPattern(CountPattern countPattern) throws TeddyException {
    String targetColName = countPattern.getCol();
    int targetColno = -1;
    Expression expr = countPattern.getOn();
    Boolean ignoreCase = countPattern.getIgnoreCase();
    int rowno, colno;

    if (!colNames.contains(targetColName)) {
      throw new TeddyException("doCountPattern(): column not found: " + targetColName);
    } else {
      for (colno = 0; colno < colCnt; colno++) {
        if (colNames.get(colno).equals(targetColName)) {
          if (colTypes.get(colno) != TYPE.STRING) {
            throw new TeddyException("doCountPattern(): works only on STRING: " + colTypes.get(colno));
          }
          targetColno = colno;
        }
      }
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    String newColName = checkNewColName("countpattern_" + targetColName, true);
    newDf.colCnt++;
    newDf.colNames.add(newColName);
    newDf.colTypes.add(TYPE.LONG);

    String patternStr;
    if (expr instanceof Constant.StringExpr) {
      patternStr = ((Constant.StringExpr) expr).getEscapedValue();
      if (ignoreCase != null && ignoreCase) {
        String ignorePatternStr = "";
        for (int i = 0; i < patternStr.length(); i++) {
          ignorePatternStr += "[";
          String c = String.valueOf(patternStr.charAt(i));
          ignorePatternStr += c.toUpperCase() + c.toLowerCase();
          ignorePatternStr += "]";
        }
        patternStr = ignorePatternStr;
      }
    } else if (expr instanceof RegularExpr) {
      patternStr = ((RegularExpr) expr).getEscapedValue().replaceAll("[\\\\]+", "\\\\");
    } else {
      throw new TeddyException("doCountPattern(): illegal pattern type: " + expr.toString());
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
      long count = 0;
      while (matcher.find()) {
        count++;
      }
      newRow.add(newColName, count);
      newDf.objGrid.add(newRow);
    }

    return newDf;
  }

  public DataFrame doReplace(Replace replace) throws TeddyException {
    Expression targetColExpr = replace.getCol();
    String targetColName;
    Expression expr = replace.getOn();
    Expression withExpr = replace.getWith();
    String withExprStr;
    Boolean global = replace.getGlobal();
    int rowno;

    if (!(targetColExpr instanceof Identifier.IdentifierExpr)) {
      throw new TeddyException("doReplace(): wrong target column expression: " + targetColExpr.toString());
    }
    targetColName = targetColExpr.toString();
    withExprStr = (String)eval(withExpr, 0);  // TODO; eval이 Row를 받도록.

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);
    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      newDf.objGrid.add(objGrid.get(rowno));
    }

    String patternStr;
    if (expr instanceof Constant.StringExpr) {
      patternStr = ((Constant.StringExpr) expr).getEscapedValue();
    } else if (expr instanceof RegularExpr) {
      patternStr = ((RegularExpr) expr).getEscapedValue().replaceAll("[\\\\]+", "\\\\");
    } else {
      throw new TeddyException("deReplace(): illegal pattern type: " + expr.toString());
    }

    for (rowno = 0; rowno < newDf.objGrid.size(); rowno++) {
      Row row = newDf.objGrid.get(rowno);
      String targetStr = (String) row.get(targetColName);
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(targetStr);
      if (matcher.find()) {
        if (global) {
          row.set(targetColName, matcher.replaceAll(stripSingleQuote((String) eval(withExpr, rowno))));
        } else {
          row.set(targetColName, matcher.replaceFirst(stripSingleQuote((String) eval(withExpr, rowno))));
        }
      }
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

  private String checkRightColName(String rightColName) throws TeddyException {
    if (colNames.contains(rightColName)) {
      return checkRightColName("r_" + rightColName);
    }
    return rightColName;
  }

  private String checkNewColName(String newColName, boolean convert) throws TeddyException {
    if (colNames.contains(newColName)) {
      if (convert) {
        return checkNewColName(newColName + "_1", convert);
      }
      throw new TeddyException("colNameCheck(): column name exists: " + newColName);
    }
    return newColName;
  }

  private String stripDoubleQuote(String str) {
    return str.substring(str.indexOf('"') + 1, str.lastIndexOf('"'));
  }

  private String stripSingleQuote(String str) {
    return str.substring(str.indexOf("'") + 1, str.lastIndexOf("'"));
  }

  public DataFrame doUnnest(Unnest unnest) throws TeddyException {
    String targetColName = unnest.getCol();
    int targetColno = -1;
    Expression idx = unnest.getIdx();
    int rowno, colno;

    for (colno = 0; colno < colCnt; colno++) {
      if (colNames.get(colno).equals(targetColName)) {
        if (colTypes.get(colno) != TYPE.ARRAY && colTypes.get(colno) != TYPE.MAP) {
          throw new TeddyException("doUnnest(): works only on ARRAY/MAP: " + colTypes.get(colno));
        }
        targetColno = colno;
        break;
      }
    }
    if (colno == colCnt) {
      throw new TeddyException("doUnnest(): column not found: " + targetColName);
    }

    int arrayIdx = -1;
    String mapKey = null;
    String newColName;

    if (colTypes.get(targetColno) == TYPE.ARRAY) {
      // 컬럼이름은 언제나 unnest_0
      // row별로 fetch는 arrayIdx로
      if (idx instanceof Constant.StringExpr) {   // supports StringExpr for backward-compatability
        arrayIdx = Integer.valueOf(((Constant.StringExpr) idx).getEscapedValue());
      } else if (idx instanceof Constant.LongExpr) {
        arrayIdx = ((Long)((Constant.LongExpr) idx).getValue()).intValue();
      } else {
        throw new TeddyException("doUnnest(): invalid index type: " + idx.toString());
      }
      newColName = "unnest_0";
    } else {
      // row별로 fetch는 mapKey로
      // 컬럼이름은 mapKey를 기존컬럼과 안겹치게 변형한 것
      if (idx instanceof Identifier.IdentifierExpr) {
        throw new TeddyException("doUnnest(): idx on MAP type should be STRING (maybe, this is a column name): " + ((Identifier.IdentifierExpr) idx).getValue());
      } else if (idx instanceof Constant.StringExpr) {
        mapKey = ((Constant.StringExpr) idx).getEscapedValue();
        newColName = checkNewColName("unnest_" + mapKey, true);
      } else {
        throw new TeddyException("doUnnest(): idx on MAP type should be STRING: " + idx.toString());
      }
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    newDf.colCnt++;
    newDf.colNames.add(newColName);
    newDf.colTypes.add(TYPE.STRING);

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (colno = 0; colno < colCnt; colno++) {
        newRow.add(colNames.get(colno), row.get(colno));
      }

      if (newDf.colTypes.get(targetColno) == TYPE.ARRAY) {
        String csv = ((String)row.get(targetColno)).substring(1);
        csv = csv.substring(0, csv.length() - 1);
        String[] values = csv.split(",");
        if (arrayIdx >= values.length) {
          throw new TeddyException(String.format("doUnnest(): arrayIdx > array length: idx=%d len=%d rowno=%d", arrayIdx, values.length, rowno));
        }
        newRow.add(newColName, stripDoubleQuote(values[arrayIdx]));
      } else {
        String col = null;
        Map<String, Object> map;
        try {
          col = (String) row.get(targetColno);
          map = new ObjectMapper().readValue((String) col, HashMap.class);
        } catch (JsonParseException e) {
          String msg = "doUnnest(): invalid JSON: col=" + col;
          LOGGER.error(msg);
          throw new TeddyException(msg);
        } catch (JsonMappingException e) {
          String msg = String.format("doUnnest(): cannot map JSON: mapKey=%s col=%s", mapKey, col);
          LOGGER.error(msg, e);
          throw new TeddyException(msg);
        } catch (IOException e) {
          String msg = "doUnnest(): IOException: mapKey=" + mapKey;
          LOGGER.error(msg);
          throw new TeddyException(msg);
        }
        if (!map.containsKey(mapKey)) {
          throw new TeddyException("doUnnest(): MAP value doesn't have requested key: " + mapKey);
        }
        newRow.add(newColName, map.get(mapKey));
      }
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public DataFrame doFlatten(Flatten flatten) throws TeddyException {
    String targetColName = flatten.getCol();
    int targetColno = -1;
    int colno;

    for (colno = 0; colno < colCnt; colno++) {
      if (colNames.get(colno).equals(targetColName)) {
        if (colTypes.get(colno) != TYPE.ARRAY) {
          throw new TeddyException("doFlatten(): works only on ARRAY: " + colTypes.get(colno));
        }
        targetColno = colno;
        break;
      }
    }
    if (colno == colCnt) {
      throw new TeddyException("doFlatten(): column not found: " + targetColName);
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    for (colno = 0; colno < colNames.size(); colno++) {
      if (colNames.get(colno).equals(targetColName)) {
        newDf.colTypes.add(TYPE.STRING);
      } else {
        newDf.colTypes.add(colTypes.get(colno));
      }
    }

    Iterator<Row> iter = objGrid.iterator();
    Row row = null;     // of aggregatedDf
    Row newRow = null;  // of pivotedDf
    while (iter.hasNext()) {
      row = iter.next();  // of aggregatedDf

      String csv = ((String)row.get(targetColno)).substring(1);
      csv = csv.substring(0, csv.length() - 1);
      String[] values = csv.split(",");
      for (int i = 0; i < values.length; i++) {
        String value = stripDoubleQuote(values[i]);
        newRow = new Row();
        for (colno = 0; colno < colNames.size(); colno++) {
          String colName = colNames.get(colno);
          if (colno == targetColno) {
            newRow.add(colName, value);
          } else {
            newRow.add(colName, row.get(colNames.get(colno)));
          }
        }
        newDf.objGrid.add(newRow);
      }
    }
    return newDf;
  }

  public DataFrame doMerge(Merge merge) throws TeddyException {
    Expression targetExpr = merge.getCol();
    String with = stripSingleQuote(merge.getWith());
    String as = stripSingleQuote(merge.getAs());
    int rowno, colno;

    String newColName = checkNewColName(as, true);

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    newDf.colCnt++;
    newDf.colNames.add(newColName);
    newDf.colTypes.add(TYPE.STRING);

    List<String> targetColNames = null;
    if (targetExpr instanceof Identifier.IdentifierExpr) {
      targetColNames = new ArrayList<>();
      targetColNames.add(((Identifier.IdentifierExpr) targetExpr).getValue());
    } else if (targetExpr instanceof Identifier.IdentifierArrayExpr) {
      targetColNames = ((Identifier.IdentifierArrayExpr) targetExpr).getValue();
    }

    if (targetColNames.size() == 0) {
      throw new TeddyException("doMerge(): no input column designated");
    }

    for (String colName : targetColNames) {
      if (!colNames.contains(colName)) {
        throw new TeddyException("doMerge(): column not found: " + colName);
      }
    }

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (colno = 0; colno < colCnt; colno++) {
        newRow.add(colNames.get(colno), row.get(colno));
      }
      StringBuilder sb = new StringBuilder();
      sb.append(row.get(targetColNames.get(0)));
      for (int i = 1; i < targetColNames.size(); i++) {
        sb.append(with).append(row.get(targetColNames.get(i)));
      }
      newRow.add(as, sb.toString());
      newDf.objGrid.add(newRow);
    }
    return newDf;
  }

  public DataFrame doSplit(Split split) throws TeddyException {
    String targetColName = split.getCol();
    Expression expr = split.getOn();
    int limit = split.getLimit();
    Boolean ignoreCase = split.getIgnoreCase();
    int targetColno = -1;
    int rowno, colno;

    for (colno = 0; colno < colCnt; colno++) {
      if (colNames.get(colno).equals(targetColName)) {
        if (colTypes.get(colno) != TYPE.STRING) {
          throw new TeddyException("doSplit(): works only on STRING: " + colTypes.get(colno));
        }
        targetColno = colno;
        break;
      }
    }

    if (colno == colCnt) {
      throw new TeddyException("doSplit(): column not found: " + targetColName);
    }
    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    List<String> newColNames = new ArrayList<>();
    for (int i = 0; i <= limit; i++) {
      String newColName = checkNewColName("split_" + targetColName + (i + 1), true);
      newColNames.add(newColName);
      newDf.colNames.add(newColName);
      newDf.colTypes.add(TYPE.STRING);
      newDf.colCnt++;
    }

    String patternStr;
    if (expr instanceof Constant.StringExpr) {
      patternStr = ((Constant.StringExpr) expr).getEscapedValue();
      if (ignoreCase != null && ignoreCase) {
        String ignorePatternStr = "";
        for (int i = 0; i < patternStr.length(); i++) {
          ignorePatternStr += "[";
          String c = String.valueOf(patternStr.charAt(i));
          ignorePatternStr += c.toUpperCase() + c.toLowerCase();
          ignorePatternStr += "]";
        }
        patternStr = ignorePatternStr;
      }
    } else if (expr instanceof RegularExpr) {
      patternStr = ((RegularExpr) expr).getEscapedValue().replaceAll("[\\\\]+", "\\\\");
    } else {
      throw new TeddyException("doSplit(): illegal pattern type: " + expr.toString());
    }

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      Row newRow = new Row();
      for (colno = 0; colno < colCnt; colno++) {
        newRow.add(colNames.get(colno), row.get(colno));
      }

      String targetStr = (String)row.get(targetColno);
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(targetStr);
      int curPos = 0;
      boolean lastSaved = false;
      for (int i = 0; i <= limit; i++) {
        if (matcher.find()) {
          String newColData = targetStr.substring(curPos, matcher.start());
          curPos = matcher.end();
          newRow.add(newColNames.get(i), newColData);
        } else {
          if (!lastSaved) {
            String newColData = targetStr.substring(curPos);
            newRow.add(newColNames.get(i), newColData);
            lastSaved = true;
          } else {
            newRow.add(newColNames.get(i), "");
          }
        }
      }
      newDf.objGrid.add(newRow);
    }

    return newDf;
  }

  public DataFrame doAggregateInternal(List<String> groupByColNames, List<String> targetExprStrs) throws TeddyException {
    List<Integer> groupByColnos = new ArrayList<>();
    List<Integer> targetAggrColnos = new ArrayList<>();   // 각 aggrValue는 1개의 target column을 가짐
    List<AggrType> targetAggrTypes = new ArrayList<>();
    List<String> resultColNames = new ArrayList<>();
    List<TYPE> resultColTypes = new ArrayList<>();
    Map<Object, Object> groupByBuckets = new HashMap<>();
    int rowno, colno;

    // aggregation expression strings -> target aggregation types, target colnos, result colnames, result coltypes
    for (int i = 0; i < targetExprStrs.size(); i++) {
      String targetExprStr = stripSingleQuote(targetExprStrs.get(i));
      AggrType aggrType;
      String targetColName;
      if (targetExprStr.toUpperCase().startsWith("COUNT")) {
        aggrType = AggrType.COUNT;
        resultColNames.add(checkNewColName("count", true));
        resultColTypes.add(TYPE.LONG);
      } else {
        Pattern pattern = Pattern.compile("\\w+\\((\\w+)\\)");
        Matcher matcher = pattern.matcher(targetExprStr);
        if (matcher.find() == false) {
          throw new TeddyException("doAggregateInternal(): invalid aggregation function expression: " + targetExprStr.toString());
        }

        if (targetExprStr.toUpperCase().startsWith(AggrType.SUM.name())) {
          aggrType = AggrType.SUM;
        } else if (targetExprStr.toUpperCase().startsWith(AggrType.AVG.name())) {
          aggrType = AggrType.AVG;
        } else if (targetExprStr.toUpperCase().startsWith(AggrType.MIN.name())) {
          aggrType = AggrType.MIN;
        } else if (targetExprStr.toUpperCase().startsWith(AggrType.MAX.name())) {
          aggrType = AggrType.MAX;
        } else {
          throw new TeddyException("doAggregateInternal(): aggregation column not found: " + targetExprStr);
        }

        targetColName = matcher.group(1);
        for (colno = 0; colno < colCnt; colno++) {
          String colName = colNames.get(colno);
          if (colName.equals(targetColName)) {
            targetAggrColnos.add(colno);
            resultColNames.add(checkNewColName(aggrType.name().toLowerCase() + "_" + colName, true));
            resultColTypes.add((aggrType == AggrType.AVG) ? TYPE.DOUBLE : getTypeOfColumn(colName));
            break;
          }
        }
        if (colno == colCnt) {
          throw new TeddyException("doAggregateInternal(): aggregation target column not found: " + targetColName);
        }
      }
      targetAggrTypes.add(aggrType);
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = groupByColNames.size() + targetAggrTypes.size();

    // 지금 twinkle 코드에 맞춰서 aggregation 값들을 먼저 배치
    for (int i = 0; i < resultColNames.size(); i++) {
      newDf.colNames.add(resultColNames.get(i));
      newDf.colTypes.add(resultColTypes.get(i));
    }

    // group by colnames existence check & append to result colnames/coltypes
    for (int i = 0; i < groupByColNames.size(); i++) {
      String groupByColName = groupByColNames.get(i);
      for (colno = 0; colno < colCnt; colno++) {
        if (colNames.get(colno).equals(groupByColName)) {
          groupByColnos.add(colno);
          newDf.colNames.add(groupByColName);
          newDf.colTypes.add(colTypes.get(colno));
          break;
        }
      }
      if (colno == colCnt) {
        throw new TeddyException("doAggregateInternal(): group by column not found: " + groupByColName);
      }
    }

    for (rowno = 0; rowno < objGrid.size(); rowno++) {
      Row row = objGrid.get(rowno);
      List<Object> groupByKey = new ArrayList<>();
      for (int i = 0; i < groupByColnos.size(); i++) {
        groupByKey.add(row.get(groupByColnos.get(i)));
      }

      if (groupByBuckets.containsKey(groupByKey)) {
        List<Object> aggregatedValues = (List<Object>) groupByBuckets.get(groupByKey);
        for (int j = 0; j < targetAggrTypes.size(); j++) {
          if (targetAggrTypes.get(j) == AggrType.AVG) {
            Map<String, Object> avgObj = (Map<String, Object>) aggregatedValues.get(j);
            avgObj.put("count", (Long)avgObj.get("count") + 1);
            if (resultColTypes.get(j) == TYPE.LONG) {
              avgObj.put("sum", (Long)avgObj.get("sum") + (Long)row.get(targetAggrColnos.get(j)));
            } else {
              if (colTypes.get(targetAggrColnos.get(j)) == TYPE.LONG) {
                avgObj.put("sum", (Double)avgObj.get("sum") + Double.valueOf((Long)row.get(targetAggrColnos.get(j))));
              } else {
                avgObj.put("sum", (Double) avgObj.get("sum") + (Double) row.get(targetAggrColnos.get(j)));
              }
            }
            aggregatedValues.set(j, avgObj);
          }
          else if (targetAggrTypes.get(j) == AggrType.COUNT) {
            aggregatedValues.set(j, (Long)aggregatedValues.get(j) + 1);
          }
          else if (targetAggrTypes.get(j) == AggrType.SUM) {
            if (resultColTypes.get(j) == TYPE.LONG) {
              aggregatedValues.set(j, (Long)aggregatedValues.get(j) + (Long)row.get(targetAggrColnos.get(j)));
            } else {
              aggregatedValues.set(j, (Double)aggregatedValues.get(j) + (Double)row.get(targetAggrColnos.get(j)));
            }
          }
          else if (targetAggrTypes.get(j) == AggrType.MIN) {
            if (resultColTypes.get(j) == TYPE.LONG) {
              Long newValue = (Long)row.get(targetAggrColnos.get(j));
              if (newValue < (Long)aggregatedValues.get(j)) {
                aggregatedValues.set(j, newValue);
              }
            } else {
              Double newValue = (Double)row.get(targetAggrColnos.get(j));
              if (newValue < (Double)aggregatedValues.get(j)) {
                aggregatedValues.set(j, newValue);
              }
            }
          }
          else if (targetAggrTypes.get(j) == AggrType.MAX) {
            if (resultColTypes.get(j) == TYPE.LONG) {
              Long newValue = (Long)row.get(targetAggrColnos.get(j));
              if (newValue > (Long)aggregatedValues.get(j)) {
                aggregatedValues.set(j, newValue);
              }
            } else {
              Double newValue = (Double)row.get(targetAggrColnos.get(j));
              if (newValue > (Double)aggregatedValues.get(j)) {
                aggregatedValues.set(j, newValue);
              }
            }
          }
        }
        groupByBuckets.put(groupByKey, aggregatedValues);
      } // end of containes groupByKey
      else {  // belows are for new groupByKey
        List<Object> aggregatedValues = new ArrayList<>();
        for (int j = 0; j < targetAggrTypes.size(); j++) {
          if (targetAggrTypes.get(j) == AggrType.AVG) {
            Map<String, Object> avgObj = new HashedMap();
            avgObj.put("count", Long.valueOf(1));
            if (colTypes.get(targetAggrColnos.get(j)) == TYPE.LONG) {
              avgObj.put("sum", Double.valueOf((Long)row.get(targetAggrColnos.get(j))));
            } else {
              avgObj.put("sum", row.get(targetAggrColnos.get(j)));
            }
            aggregatedValues.add(avgObj);
          }
          else if (targetAggrTypes.get(j) == AggrType.COUNT) {
            aggregatedValues.add(Long.valueOf(1));
          }
          else {
            aggregatedValues.add(row.get(targetAggrColnos.get(j)));
          }
        }
        groupByBuckets.put(groupByKey, aggregatedValues);
      }
    }

    for (Map.Entry<Object, Object> elem : groupByBuckets.entrySet()) {
      Row newRow = new Row();
      List<Object> aggregatedValues = (List<Object>)elem.getValue();
      for (int i = 0; i < aggregatedValues.size(); i++) {
        if (targetAggrTypes.get(i) == AggrType.AVG) {
          Map<String, Object> avgObj = (Map<String, Object>) aggregatedValues.get(i);
          Double sum = (Double)avgObj.get("sum");
          Long count = (Long)avgObj.get("count");
          Double avg = BigDecimal.valueOf(sum / count).setScale(2, RoundingMode.HALF_UP).doubleValue();
          newRow.add(resultColNames.get(i), avg);
        } else {
          newRow.add(resultColNames.get(i), aggregatedValues.get(i));
        }
      }

      int i = 0;
      for (Object groupByValue : (List<Object>) elem.getKey()) {
        newRow.add(groupByColNames.get(i++), groupByValue);
      }

      newDf.objGrid.add(newRow);
    }

    return newDf;
  }

  public DataFrame doAggregate(Aggregate aggregate) throws TeddyException {
    Expression groupByColExpr = aggregate.getGroup();
    Expression aggrValueExpr = aggregate.getValue();
    List<String> groupByColNames = new ArrayList<>();
    List<String> targetExprStrs = new ArrayList<>();      // sum(x), avg(x), count() 등의 expression string

    // group by expression -> group by colnames
    if (groupByColExpr instanceof Identifier.IdentifierExpr) {
      groupByColNames.add(((Identifier.IdentifierExpr) groupByColExpr).getValue());
    } else if (groupByColExpr instanceof Identifier.IdentifierArrayExpr) {
      groupByColNames.addAll(((Identifier.IdentifierArrayExpr) groupByColExpr).getValue());
    } else {
      throw new TeddyException("doAggregate(): invalid group by column expression type: " + groupByColExpr.toString());
    }

    // aggregation value expression -> aggregation expression strings
    if (aggrValueExpr instanceof Constant.StringExpr) {
      targetExprStrs.add((String)(((Constant.StringExpr) aggrValueExpr).getValue()));
    } else if (aggrValueExpr instanceof Constant.ArrayExpr) {
      for (Object obj : ((Constant.ArrayExpr) aggrValueExpr).getValue()) {
        String strAggrValue = (String)obj;
        targetExprStrs.add(strAggrValue);
      }
    } else {
      throw new TeddyException("doAggregate(): invalid aggregation value expression type: " + aggrValueExpr.toString());
    }

    return doAggregateInternal(groupByColNames, targetExprStrs);
  }

  private Map<String, Object> buildGroupByKey(Row row, List<String> groupByColNames) {
    Map<String, Object> groupByKey = new HashMap<>();
    for (String groupByColName : groupByColNames) {
      groupByKey.put(groupByColName, row.get(groupByColName));
    }
    return groupByKey;
  }

  private boolean groupByKeyChanged(Row row, List<String> groupByColNames, Map<String, Object> groupByKey) {
    for (String groupByColName : groupByColNames) {
      if (!groupByKey.get(groupByColName).equals(row.get(groupByColName))) {
        return true;
      }
    }
    return false;
  }

  // row: row from aggregatedDf
  private Row newPivotRow(Row row, List<String> colNames, List<TYPE> colTypes, List<String> groupByColNames) throws TeddyException {
    Row newRow = new Row();
    int colno;

    for (String groupByColName : groupByColNames) {
      newRow.add(groupByColName, row.get(groupByColName));
    }

    // 일단 기본값으로 깔고, 실제 있는 값을 채우기로 함
    for (colno = groupByColNames.size(); colno < colNames.size(); colno++) {
      TYPE colType = colTypes.get(colno);
      switch (colType) {
        case DOUBLE:
          newRow.add(colNames.get(colno), Double.valueOf(0));
          break;
        case LONG:
          newRow.add(colNames.get(colno), Long.valueOf(0));
          break;
        default:
          throw new TeddyException("doPivot(): column type of aggregation value should be DOUBLE or LONG: " + colType);
      }
    }
    return newRow;
  }

  private String buildPivotNewColName(AggrType aggrType, String aggrTargetColName,
                                      List<String> pivotColNames, Row row) {
    String newColName = null;

    switch (aggrType) {
      case COUNT:
        newColName = "row_count";
        break;
      case SUM:
        newColName = "sum_" + aggrTargetColName;
        break;
      case AVG:
        newColName = "avg_" + aggrTargetColName;
        break;
      case MIN:
        newColName = "min_" + aggrTargetColName;
        break;
      case MAX:
        newColName = "max_" + aggrTargetColName;
        break;
    }

    for (String pivotColName : pivotColNames) {
      newColName += "_" + row.get(pivotColName);
    }
    return newColName;
  }

  public DataFrame doPivot(Pivot pivot) throws TeddyException {
    Expression pivotColExpr = pivot.getCol();
    Expression groupByColExpr = pivot.getGroup();
    Expression aggrValueExpr = pivot.getValue();
    List<String> pivotColNames = new ArrayList<>();
    List<String> groupByColNames = new ArrayList<>();
    List<String> aggrValueStrs = new ArrayList<>();      // sum(x), avg(x), count() 등의 expression string
    List<AggrType> aggrTypes = new ArrayList<>();
    List<String> aggrTargetColNames = new ArrayList<>();
    int rowno, colno;

    // group by expression -> group by colnames
    if (groupByColExpr instanceof Identifier.IdentifierExpr) {
      groupByColNames.add(((Identifier.IdentifierExpr) groupByColExpr).getValue());
    } else if (groupByColExpr instanceof Identifier.IdentifierArrayExpr) {
      groupByColNames.addAll(((Identifier.IdentifierArrayExpr) groupByColExpr).getValue());
    } else {
      throw new TeddyException("doPivot(): invalid group by column expression type: " + groupByColExpr.toString());
    }

    // pivot target (to-be-column) column expression -> group by colnames
    if (pivotColExpr instanceof Identifier.IdentifierExpr) {
      pivotColNames.add(((Identifier.IdentifierExpr) pivotColExpr).getValue());
    } else if (pivotColExpr instanceof Identifier.IdentifierArrayExpr) {
      pivotColNames.addAll(((Identifier.IdentifierArrayExpr) pivotColExpr).getValue());
    } else {
      throw new TeddyException("doPivot(): invalid pivot column expression type: " + pivotColExpr.toString());
    }

    // aggregation value expression -> aggregation expression strings
    if (aggrValueExpr instanceof Constant.StringExpr) {
      aggrValueStrs.add((String) (((Constant.StringExpr) aggrValueExpr).getValue()));
    } else if (aggrValueExpr instanceof Constant.ArrayExpr) {
      for (Object obj : ((Constant.ArrayExpr) aggrValueExpr).getValue()) {
        String strAggrValue = (String) obj;
        aggrValueStrs.add(strAggrValue);
      }
    } else {
      throw new TeddyException("doPivot(): invalid aggregation value expression type: " + aggrValueExpr.toString());
    }

    List<String> mergedGroupByColNames = new ArrayList<>();
    mergedGroupByColNames.addAll(pivotColNames);
    mergedGroupByColNames.addAll(groupByColNames);

    DataFrame aggregatedDf = doAggregateInternal(mergedGroupByColNames, aggrValueStrs);

    // <aggregatedDf>
    // +--------------+--------------+----------+----------+------------+------------+
    // | pivotTarget1 | pivotTarget2 | groupBy1 | groupBy2 | aggrValue1 | aggrValue2 |
    // +--------------+--------------+----------+----------+------------+------------+
    // |     year     |     month    | priority |  status  | sum(price) |   count()  |
    // +--------------+--------------+----------+----------+------------+------------+
    // <pivotedDf>
    // +----------+--------+-------------------+-------------------+-----+-------------------+-------------------+-----+
    // | priority | status | sum_price_1992_01 | sum_price_1992_02 | ... | row_count_1992_01 | row_count_1992_02 | ... |
    // +----------+--------+-------------------+-------------------+-----+-------------------+-------------------+-----+

    DataFrame pivotedDf = new DataFrame();
    pivotedDf.colCnt = groupByColNames.size();

    // 일단 group by column은 column에 추가
    pivotedDf.colNames.addAll(groupByColNames);
    for (String colName : groupByColNames) {
      pivotedDf.colTypes.add(getTypeOfColumn(colName));
    }

    // pivot column을 추가: aggrType은 prefix, distinct value는 surfix -> pivotColNames로 sort해서 진행
    aggregatedDf = aggregatedDf.doSortInternal(pivotColNames);

    for (int i = 0; i < aggrValueStrs.size(); i++) {
      String aggrValueStr = stripSingleQuote(aggrValueStrs.get(i));
      AggrType aggrType;
      TYPE newColType = TYPE.INVALID;
      String newColName;
      String aggrTargetColName = null;

      if (aggrValueStr.toUpperCase().startsWith("COUNT")) {
        aggrType = AggrType.COUNT;
        newColType = TYPE.LONG;
      } else {
        if (aggrValueStr.toUpperCase().startsWith(AggrType.SUM.name())) {
          aggrType = AggrType.SUM;
        } else if (aggrValueStr.toUpperCase().startsWith(AggrType.AVG.name())) {
          aggrType = AggrType.AVG;
        } else if (aggrValueStr.toUpperCase().startsWith(AggrType.MIN.name())) {
          aggrType = AggrType.MIN;
        } else if (aggrValueStr.toUpperCase().startsWith(AggrType.MAX.name())) {
          aggrType = AggrType.MAX;
        } else {
          throw new TeddyException("doAggregateInternal(): unsupported aggregation function: " + aggrValueStr);
        }

        Pattern pattern = Pattern.compile("\\w+\\((\\w+)\\)");
        Matcher matcher = pattern.matcher(aggrValueStr);
        if (matcher.find() == false) {
          throw new TeddyException("doAggregateInternal(): wrong aggregation function expression: " + aggrValueStr);
        }

        aggrTargetColName = matcher.group(1);
        for (colno = 0; colno < colCnt; colno++) {
          String colName = colNames.get(colno);
          if (colName.equals(aggrTargetColName)) {
            newColType = (aggrType == AggrType.AVG) ? TYPE.DOUBLE : getTypeOfColumn(colName);
            break;
          }
        }
        if (colno == colCnt) {
          throw new TeddyException("doAggregateInternal(): aggregation target column not found: " + aggrTargetColName);
        }
      }

      Map<String, Object> pivotColGroupKey = null;
      for (Row row : aggregatedDf.objGrid) {
        if (pivotColGroupKey == null || groupByKeyChanged(row, pivotColNames, pivotColGroupKey)) {
          newColName = buildPivotNewColName(aggrType, aggrTargetColName, pivotColNames, row);

          pivotedDf.colCnt++;
          pivotedDf.colNames.add(checkNewColName(newColName, true));
          pivotedDf.colTypes.add(newColType);

          pivotColGroupKey = buildGroupByKey(row, pivotColNames);
        }
        if (pivotColGroupKey == null) {
          pivotColGroupKey = buildGroupByKey(row, pivotColNames);
        }
      }

      if (pivotedDf.colCnt > 1000) {
        throw new TeddyException("doPivot(): too many pivoted column count: " + pivotedDf.colCnt);
      }

      aggrTypes.add(aggrType);
      aggrTargetColNames.add(aggrTargetColName);
    }

    // group by column, pivot column들을 모두 포함한 row들 생성  -> groupByColNames으로 sort해서 진행
    // aggregatedDf의 row가 더 남지 않을 때까지  를 모두 돌아갈 때까지 pivotDf를 만듦
    aggregatedDf = aggregatedDf.doSortInternal(groupByColNames);
    Map<String, Object> groupByKey = null;

    Iterator<Row> iter = aggregatedDf.objGrid.iterator();
    Row row = null;     // of aggregatedDf
    Row newRow = null;  // of pivotedDf
    while (iter.hasNext()) {
      row = iter.next();  // of aggregatedDf
      if (groupByKey == null) {
        newRow = newPivotRow(row, pivotedDf.colNames, pivotedDf.colTypes, groupByColNames);
        groupByKey = buildGroupByKey(row, groupByColNames);
      } else if (groupByKeyChanged(row, groupByColNames, groupByKey)) {
        pivotedDf.objGrid.add(newRow);
        newRow = newPivotRow(row, pivotedDf.colNames, pivotedDf.colTypes, groupByColNames);
        groupByKey = buildGroupByKey(row, groupByColNames);
      }

      List<String> aggregatedDfColNames = new ArrayList<>();
      for (colno = 0; colno < aggrTargetColNames.size(); colno++) {
        aggregatedDfColNames.add(aggregatedDf.colNames.get(colno));
      }
      for (int i = 0; i < aggrTargetColNames.size(); i++) {
        String aggrTargetColName = aggrTargetColNames.get(i);
        newRow.set(buildPivotNewColName(aggrTypes.get(i), aggrTargetColName, pivotColNames, row),
                   row.get(i));
      }
    }
    pivotedDf.objGrid.add(newRow);

    return pivotedDf;
  }

  public DataFrame doUnpivot(Unpivot unpivot) throws TeddyException {
    Expression unpivotColExpr = unpivot.getCol();
    int groupEvery = unpivot.getGroupEvery();
    List<String> unpivotColNames = new ArrayList<>();
    List<String> fixedColNames = new ArrayList<>();

    // group by expression -> group by colnames
    if (unpivotColExpr instanceof Identifier.IdentifierExpr) {
      unpivotColNames.add(((Identifier.IdentifierExpr) unpivotColExpr).getValue());
    } else if (unpivotColExpr instanceof Identifier.IdentifierArrayExpr) {
      unpivotColNames.addAll(((Identifier.IdentifierArrayExpr) unpivotColExpr).getValue());
    } else {
      throw new TeddyException("doUnpivot(): invalid unpivot target column expression type: " + unpivotColExpr.toString());
    }

    // unpivot target이 존재하는지 체크
    for (String colName : unpivotColNames) {
      if (!colName.contains(colName)) {
        throw new TeddyException("doUnpivot(): column not found: " + colName);
      }
    }

    // 고정 column 리스트 확보
    for (String colName : colNames) {
      if (!unpivotColNames.contains(colName)) {
        fixedColNames.add(colName);
      }
    }

    DataFrame newDf = new DataFrame();
    newDf.colCnt = fixedColNames.size() + groupEvery * 2;
    for (int i = 0; i < fixedColNames.size(); i++) {
      String colName = fixedColNames.get(i);
      newDf.colNames.add(colName);
      newDf.colTypes.add(getTypeOfColumn(colName));
    }
    for (int i = 0; i < unpivotColNames.size(); i++) {
      String unpivotColName = unpivotColNames.get(i);
      TYPE unpivotColType = getTypeOfColumn(unpivotColName);
      newDf.colNames.add("key" + (i + 1));
      newDf.colTypes.add(TYPE.STRING);
      newDf.colNames.add("value" + (i + 1));
      newDf.colTypes.add(unpivotColType);

      // groupEvery가 1인 경우 key1, value1만 사용함.
      // 이 경우, 모든 unpivot 대상 column의 TYPE이 같아야함.
      if (groupEvery == 1) {
        for (i++; i < unpivotColNames.size(); i++) {
          unpivotColName = unpivotColNames.get(i);
          if (unpivotColType != getTypeOfColumn(unpivotColName)) {
            throw new TeddyException(String.format(
                    "doUnpivot(): unpivot target column types differ: %s != %s",
                    unpivotColType, getTypeOfColumn(unpivotColName)));
          }
        }
        break;
      }
    }

    Iterator<Row> iter = objGrid.iterator();
    Row row = null;     // of aggregatedDf
    Row newRow = null;  // of pivotedDf
    while (iter.hasNext()) {
      row = iter.next();  // of aggregatedDf
      newRow = new Row();
      for (String fixedColName : fixedColNames) {
        newRow.add(fixedColName, row.get(fixedColName));
      }
      int keyNo = 1;
      for (int i = 0; i < unpivotColNames.size(); i++) {
        String unpivotColName = unpivotColNames.get(i);
        newRow.add("key" + keyNo, unpivotColName);
        newRow.add("value" + keyNo, row.get(unpivotColName));
        if (groupEvery == 1) {
          newDf.objGrid.add(newRow);
          keyNo = 1;
          newRow = new Row();
          for (String fixedColName : fixedColNames) {
            newRow.add(fixedColName, row.get(fixedColName));
          }
          continue;
        } else if (groupEvery != unpivotColNames.size()) {
          throw new TeddyException("doUnpivot(): group every count should be 1 or all: " + groupEvery);
        }
        keyNo++;
      }
      if (groupEvery != 1) {
        newDf.objGrid.add(newRow);
      }
    }
    return newDf;
  }

  private DataFrame doSortInternal(List<String> orderByColNames) throws TeddyException {
    int colno;

    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    for (Row row : objGrid) {
      newDf.objGrid.add(row);
    }

    for (Row row : newDf.objGrid) {
      row.cmpKeyIdxs = new ArrayList<>();
      row.cmpKeyTypes = new ArrayList<>();
    }

    // order by colnames existence check & append to result colnames/coltypes
    for (int i = 0; i < orderByColNames.size(); i++) {
      String orderByColName = orderByColNames.get(i);
      for (colno = 0; colno < colCnt; colno++) {
        if (colNames.get(colno).equals(orderByColName)) {
          for (Row row : newDf.objGrid) {
            row.cmpKeyIdxs.add(colno);
            row.cmpKeyTypes.add(colTypes.get(colno));
          }
          break;
        }
      }
      if (colno == colCnt) {
        throw new TeddyException("doSortInternal(): order by column not found: " + orderByColName);
      }
    }

    newDf.objGrid.sort(new Comparator<Row>() {
      @Override
      public int compare(Row row1, Row row2) {
        int result;
        for (int i = 0; i < row1.cmpKeyIdxs.size(); i++) {
          Object obj1 = row1.get(row1.cmpKeyIdxs.get(i));
          Object obj2 = row2.get(row2.cmpKeyIdxs.get(i));

          if (obj1 == null) {
            return -1;
          } else if (obj2 == null) {
            return 1;
          } else {
            TYPE colType = row1.cmpKeyTypes.get(i);
            switch(colType) {
              case STRING:
                result = ((String) obj1).compareTo((String) obj2);
                if (result != 0) {
                  return result;
                }
                break;
              case BOOLEAN:
                result = ((Boolean) obj1).compareTo((Boolean) obj2);
                if (result != 0) {
                  return result;
                }
                break;
              case LONG:
                result = ((Long) obj1).compareTo((Long) obj2);
                if (result != 0) {
                  return result;
                }
                break;
              case DOUBLE:
                result = ((Double) obj1).compareTo((Double) obj2);
                if (result != 0) {
                  return result;
                }
                break;
              default:
                try {
                  throw new TeddyException("doSortInternal(): invalid column type: " + colType.name());
                } catch (TeddyException e) {
                  e.printStackTrace();
                }
            }
          }
        }
        return 0;
      }
    });

    return newDf;
  }

  public DataFrame doSort(Sort sort) throws TeddyException {
    Expression orderByColExpr = sort.getOrder();
    List<String> orderByColNames = new ArrayList<>();

    // order by expression -> order by colnames
    if (orderByColExpr instanceof Identifier.IdentifierExpr) {
      orderByColNames.add(((Identifier.IdentifierExpr) orderByColExpr).getValue());
    } else if (orderByColExpr instanceof Identifier.IdentifierArrayExpr) {
      orderByColNames.addAll(((Identifier.IdentifierArrayExpr) orderByColExpr).getValue());
    } else {
      throw new TeddyException("doSort(): invalid order by column expression type: " + orderByColExpr.toString());
    }

    return doSortInternal(orderByColNames);
  }

  private DataFrame filter(Expression condExpr, boolean keep) throws TeddyException {
    DataFrame newDf = new DataFrame();
    newDf.colCnt = colCnt;
    newDf.colNames.addAll(colNames);
    newDf.colTypes.addAll(colTypes);

    for (int rowno = 0; rowno < objGrid.size(); rowno++) {
      if (((Long) eval(condExpr, rowno)).longValue() == ((keep) ? 1 : 0)) {
        newDf.objGrid.add(objGrid.get(rowno));
      }
    }
    return newDf;
  }

  public DataFrame doKeep(Keep keep) throws TeddyException {
    Expression condExpr = keep.getRow();
    return filter(condExpr, true);
  }

  public DataFrame doDelete(Delete delete) throws TeddyException {
    Expression condExpr = delete.getRow();
    return filter(condExpr, false);
  }
}
