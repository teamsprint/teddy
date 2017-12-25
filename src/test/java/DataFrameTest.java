import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.teddy.DataFrame;
import com.skt.metatron.teddy.TeddyException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataFrameTest {

  static String getResourcePath(String relPath, boolean fromHdfs) {
    if (fromHdfs) {
      throw new IllegalArgumentException("HDFS not supported yet");
    }
    URL url = DataFrameTest.class.getClassLoader().getResource(relPath);
    return (new File(url.getFile())).getAbsolutePath();
  }

  public static String getResourcePath(String relPath) {
    return getResourcePath(relPath, false);
  }

  static private List<String[]> gridSampleCsv;    // sample.csv (Ferrari, Jaruar, ...) (7 columns, 5 rows)
  static private List<String[]> gridContractCsv;  // ibk_contract_n10000.csv
  static private List<String[]> gridStoreCsv;     // ibk_store_n10000.csv

  @BeforeClass
  public static void setUp() throws Exception {
    gridSampleCsv = new ArrayList<>();
    gridContractCsv = new ArrayList<>();
    gridStoreCsv = new ArrayList<>();

    BufferedReader br = null;
    String line;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(getResourcePath("metatron_dataset/small/sample.csv"))));
      while ((line = br.readLine()) != null) {
        String[] strCols = line.split(cvsSplitBy);
        System.out.println(strCols);
        gridSampleCsv.add(strCols);
      }

      br = new BufferedReader(new InputStreamReader(new FileInputStream(getResourcePath("data/ibk_contract_n10000.csv"))));
      while ((line = br.readLine()) != null) {
        String[] strCols = line.split(cvsSplitBy);
        System.out.println(strCols);
        gridContractCsv.add(strCols);
      }

      br = new BufferedReader(new InputStreamReader(new FileInputStream(getResourcePath("data/ibk_store_n10000.csv"))));
      while ((line = br.readLine()) != null) {
        String[] strCols = line.split(cvsSplitBy);
        System.out.println(strCols);
        gridStoreCsv.add(strCols);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  @Test
  public void test_show() {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();

    df = new DataFrame();
    df.setGrid(gridContractCsv);
    df.show();

    df = new DataFrame();
    df.setGrid(gridStoreCsv);
    df.show();
  }

  @Test
  public void test_drop() throws TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();

//    List<String> targetColNames = new ArrayList<>();
//    targetColNames.add("column1");
//    targetColNames.add("column3");
//    targetColNames.add("column6");
//    DataFrame newDf = df.drop(targetColNames);
//    newDf.show();

    String ruleString = "drop col: column2, column3";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doDrop((Drop)rule);
    newDf.show();
  }

  @Test
  public void test_select() {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();

    List<String> targetColNames = new ArrayList<>();
    targetColNames.add("column4");
    targetColNames.add("column5");
    DataFrame newDf = df.select(targetColNames);
    newDf.show();
  }

  private DataFrame apply_rule(DataFrame df, List<String> ruleStrings) throws TeddyException {
    for (String ruleString : ruleStrings) {
      Rule rule = new RuleVisitorParser().parse(ruleString);
      switch (rule.getName()) {
        case "rename":
          df = df.doRename((Rename)rule);
          break;
        case "drop":
          df = df.doDrop((Drop)rule);
          break;
        case "set":
          df = df.doSet((Set)rule);
          break;
        case "derive":
          df = df.doDerive((Derive) rule);
          break;
        case "settype":
          df = df.doSetType((SetType)rule);
          break;
        case "header":
          df = df.doHeader((Header)rule);
          break;
        default:
          throw new TeddyException("rule not supported: " + rule.getName());
      }
    } // end of for
    return df;
  }

  private DataFrame prepare_common(DataFrame df) throws IOException, TeddyException {
    List<String> ruleStrings = new ArrayList<>();
    ruleStrings.add("rename col: column5 to: HP");
    ruleStrings.add("settype col: HP type: long");
    return apply_rule(df, ruleStrings);
//    // rename column5 -> HP
//    Rule rule = new RuleVisitorParser().parse("rename col: column5 to: HP");
//    df = df.doRename((Rename)rule);
//
//    rule = new RuleVisitorParser().parse("settype col: HP type: long");
//    return df.doSetType((SetType)rule);
  }

  @Test
  public void test_rename_settype() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);
    df.show();
  }

  @Test
  public void test_set_plus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: HP value: HP + 1000";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_minus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: HP value: HP - 300";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: HP value: HP * 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_derive_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "derive as: Turbo value: HP * 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doDerive((Derive)rule);
    newDf.show();
  }

  @Test
  public void test_set_div() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: HP value: HP / 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_type_mismatch() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(gridSampleCsv);
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: HP value: HP * '10'";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);

    try {
      df.doSet((Set) rule);
    } catch (TeddyException e) {
      System.out.println(e);
    }
  }

  @Test
  public void test_header() throws IOException, TeddyException {
    List<String> ruleStrings = new ArrayList<>();

    DataFrame store = new DataFrame();
    store.setGrid(gridStoreCsv);
    store.show();

    ruleStrings.clear();
    ruleStrings.add("header rownum: 1");
    store = apply_rule(store, ruleStrings);
    store.show();
  }

  @Test
  public void test_join() throws IOException, TeddyException {
    List<String> ruleStrings = new ArrayList<>();

    DataFrame contract = new DataFrame();
    contract.setGrid(gridContractCsv);
    contract.show();

    ruleStrings.add("rename col: column1 to: cdate");
    ruleStrings.add("drop col: column2, column9");
    ruleStrings.add("rename col: column3 to: pcode1");
    ruleStrings.add("rename col: column4 to: pcode2");
    ruleStrings.add("rename col: column5 to: pcode3");
    ruleStrings.add("rename col: column6 to: pcode4");
    ruleStrings.add("rename col: column7 to: customer_id");
    ruleStrings.add("rename col: column8 to: detail_store_code");

    ruleStrings.add("settype col: pcode1 type: long");
    ruleStrings.add("settype col: pcode2 type: long");
    ruleStrings.add("settype col: pcode3 type: long");
    ruleStrings.add("settype col: pcode4 type: long");
    ruleStrings.add("settype col: detail_store_code type: long");

    contract = apply_rule(contract, ruleStrings);
    contract.show();

    DataFrame store = new DataFrame();
    store.setGrid(gridStoreCsv);
    store.show();

    ruleStrings.clear();
    ruleStrings.add("header rownum: 1");
    ruleStrings.add("drop col: store_code, store_name");
    ruleStrings.add("settype col: detail_store_code type: long");
    store = apply_rule(store, ruleStrings);
    store.show();

    List<String> leftSelectColNames = Arrays.asList(new String[]{"cdate", "pcode1", "pcode2", "pcode3", "pcode4", "customer_id", "detail_store_code"});
    List<String> rightSelectColNames = Arrays.asList(new String[]{"detail_store_code", "customer_id", "detail_store_name"});
    DataFrame newDf = contract.join(store, leftSelectColNames, rightSelectColNames, "customer_id = customer_id", "inner");
    newDf.show();
  }
}
