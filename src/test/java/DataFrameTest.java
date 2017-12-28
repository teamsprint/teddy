import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.discovery.common.preparation.rule.Set;
import com.skt.metatron.teddy.DataFrame;
import com.skt.metatron.teddy.TeddyException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.*;

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

  private static Map<String, List<String[]>> grids = new HashMap<>();

  static int limitRowCnt = 10000;

  static private List<String[]> loadGridCsv(String alias, String path) {
    List<String[]> grid = new ArrayList<>();

    BufferedReader br = null;
    String line;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(getResourcePath(path))));
      while ((line = br.readLine()) != null) {
        String[] strCols = line.split(cvsSplitBy);
        grid.add(strCols);
        if (grid.size() == limitRowCnt)
          break;
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

    grids.put(alias, grid);
    return grid;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    loadGridCsv("sample", "metatron_dataset/small/sample.csv");
    loadGridCsv("contract", "data/ibk_contract_n10000.csv");
    loadGridCsv("store", "data/ibk_store_n10000.csv");
    loadGridCsv("store1", "data/ibk_store_n3000_1.csv");
    loadGridCsv("store2", "data/ibk_store_n3000_2.csv");
    loadGridCsv("store3", "data/ibk_store_n3000_3.csv");
    loadGridCsv("store4", "data/ibk_store_n3000_4.csv");
  }

  @Test
  public void test_show() {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df.show();

    df = new DataFrame();
    df.setGrid(grids.get("contract"));
    df.show();

    df = new DataFrame();
    df.setGrid(grids.get("store"));
    df.show();
  }

  @Test
  public void test_drop() throws TeddyException, IOException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "drop col: recent, itemNo";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doDrop((Drop)rule);
    newDf.show();
  }

  @Test
  public void test_select() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    List<String> targetColNames = new ArrayList<>();
    targetColNames.add("name");
    targetColNames.add("speed");
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

    ruleStrings.add("rename col: column1 to: launch");
    ruleStrings.add("rename col: column2 to: recent");
    ruleStrings.add("rename col: column3 to: itemNo");
    ruleStrings.add("rename col: column4 to: name");
    ruleStrings.add("rename col: column5 to: speed");
    ruleStrings.add("rename col: column6 to: price");
    ruleStrings.add("rename col: column7 to: rank");
    ruleStrings.add("settype col: itemNo type: long");
    ruleStrings.add("settype col: speed type: long");
    ruleStrings.add("settype col: price type: double");
    ruleStrings.add("settype col: rank type: long");

    return apply_rule(df, ruleStrings);
  }

  @Test
  public void test_rename_settype() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();
  }

  @Test
  public void test_set_plus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "set col: speed value: speed + 1000";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_minus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "set col: speed value: speed - 300";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df.show();
    df = prepare_common(df);

    String ruleString = "set col: speed value: speed * 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_derive_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "derive as: Turbo value: speed * 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doDerive((Derive)rule);
    newDf.show();
  }

  @Test
  public void test_set_div() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "set col: speed value: speed / 10";
    String jsonRuleString = df.parseRuleString(ruleString);
    System.out.println(jsonRuleString);
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_type_mismatch() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "set col: speed value: speed * '10'";
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
    store.setGrid(grids.get("store"));
    store.show();

    ruleStrings.clear();
    ruleStrings.add("header rownum: 1");
    store = apply_rule(store, ruleStrings);
    store.show();
  }

  private DataFrame prepare_contract(DataFrame contract) throws TeddyException {
    List<String> ruleStrings = new ArrayList<>();
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

    return apply_rule(contract, ruleStrings);
  }

  private DataFrame prepare_store(DataFrame store) throws TeddyException {
    List<String> ruleStrings = new ArrayList<>();
    ruleStrings.add("header rownum: 1");
    ruleStrings.add("drop col: store_code, store_name");
    ruleStrings.add("settype col: detail_store_code type: long");
    return apply_rule(store, ruleStrings);
  }

  @Test
  public void test_join_by_string() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract.show();

    contract = prepare_contract(contract);
    contract.show();

    DataFrame store = new DataFrame();
    store.setGrid(grids.get("store"));
    store.show();

    store = prepare_store(store);
    store.show();

    List<String> leftSelectColNames = Arrays.asList(new String[]{"cdate", "pcode1", "pcode2", "pcode3", "pcode4", "customer_id", "detail_store_code"});
    List<String> rightSelectColNames = Arrays.asList(new String[]{"detail_store_code", "customer_id", "detail_store_name"});
    DataFrame newDf = contract.join(store, leftSelectColNames, rightSelectColNames, "customer_id = customer_id", "inner", 10000);
    newDf.show();
  }

  @Test
  public void test_join_by_long() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract.show();

    contract = prepare_contract(contract);
    contract.show();

    DataFrame store = new DataFrame();
    store.setGrid(grids.get("store"));
    store.show();

    store = prepare_store(store);
    store.show();

    List<String> leftSelectColNames = Arrays.asList(new String[]{"cdate", "pcode1", "pcode2", "pcode3", "pcode4", "customer_id", "detail_store_code"});
    List<String> rightSelectColNames = Arrays.asList(new String[]{"detail_store_code", "customer_id", "detail_store_name"});
    DataFrame newDf = contract.join(store, leftSelectColNames, rightSelectColNames, "detail_store_code = detail_store_code", "inner", 10000);
    newDf.show();
  }

  @Test
  public void test_union() throws IOException, TeddyException {
    DataFrame store1 = new DataFrame();
    DataFrame store2 = new DataFrame();
    DataFrame store3 = new DataFrame();
    DataFrame store4 = new DataFrame();

    store1.setGrid(grids.get("store1"));
    store2.setGrid(grids.get("store2"));
    store3.setGrid(grids.get("store3"));
    store4.setGrid(grids.get("store4"));

    store1.show();

    store1 = prepare_store(store1);
    store2 = prepare_store(store2);
    store3 = prepare_store(store3);
    store4 = prepare_store(store4);

    store1.show();

    List<DataFrame> slaveDataFrames = new ArrayList<>();
    slaveDataFrames.add(store2);
    slaveDataFrames.add(store3);
    slaveDataFrames.add(store4);
    DataFrame newDf = store1.union(slaveDataFrames, 10000);
    newDf.show();
  }

  @Test
  public void test_extract() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_common(df);
    df.show();

    String ruleString = "extract col: name on: 'e' quote: '\"' limit: 3";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doExtract((Extract)rule);
    newDf.show();
  }

  @Test
  public void test_extract_regex() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show(100);

    String ruleString = "extract col: cdate on: /\\w+/ quote: '\"' limit: 3";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doExtract((Extract)rule);
    newDf.show();
  }

  @Test
  public void test_nest_unnest_array() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "nest col: pcode1, pcode2, pcode3, pcode4 into: array as: pcode";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doNest((Nest)rule);
    newDf.show();

    ruleString = "unnest col: pcode into: array idx: 0";  // into: is not used
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doUnnest((Unnest)rule);
    newDf.show();
  }

  @Test
  public void test_nest_unnest_map() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "nest col: pcode1, pcode2, pcode3, pcode4 into: map as: pcode";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doNest((Nest)rule);
    newDf.show();

    ruleString = "unnest col: pcode into: map idx: 'pcode3'"; // into: is not used
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doUnnest((Unnest)rule);
    newDf.show();
  }

  @Test
  public void test_merge() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "merge col: pcode1, pcode2, pcode3, pcode4 with: '_' as: 'pcode'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doMerge((Merge)rule);
    newDf.show();
  }

  @Test
  public void test_merge_split() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "merge col: pcode1, pcode2, pcode3, pcode4 with: '_' as: 'pcode'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doMerge((Merge)rule);
    newDf.show();

    ruleString = "split col: pcode on: '_' limit: 4";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doSplit((Split)rule);
    newDf.show();
  }
}
