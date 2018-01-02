import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.teddy.DataFrame;
import com.skt.metatron.teddy.TeddyException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  static DataFrame prepare_multi(DataFrame multi) throws TeddyException {
    List<String> ruleStrings = new ArrayList<>();
    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: measure type: long");
    return DataFrameTest.apply_rule(multi, ruleStrings);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    loadGridCsv("sample", "metatron_dataset/small/sample.csv");
    loadGridCsv("contract", "data/ibk_contract_n10000.csv");
    loadGridCsv("product", "data/ibk_product_n5000.csv");
    loadGridCsv("store", "data/ibk_store_n10000.csv");
    loadGridCsv("store1", "data/ibk_store_n3000_1.csv");
    loadGridCsv("store2", "data/ibk_store_n3000_2.csv");
    loadGridCsv("store3", "data/ibk_store_n3000_3.csv");
    loadGridCsv("store4", "data/ibk_store_n3000_4.csv");
    loadGridCsv("multi", "dataprep/pivot_test_multiple_column.csv");
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
    df = prepare_sample(df);
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
    df = prepare_sample(df);
    df.show();

    List<String> targetColNames = new ArrayList<>();
    targetColNames.add("name");
    targetColNames.add("speed");
    DataFrame newDf = df.select(targetColNames);
    newDf.show();
  }

  static DataFrame apply_rule(DataFrame df, List<String> ruleStrings) throws TeddyException {
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

  static DataFrame prepare_sample(DataFrame df) throws IOException, TeddyException {
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
    df = prepare_sample(df);
    df.show();
  }

  @Test
  public void test_set_plus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "set col: speed value: speed + 1000";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_minus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "set col: speed value: speed - 300";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df.show();
    df = prepare_sample(df);

    String ruleString = "set col: speed value: speed * 10";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_if() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df.show();
    df = prepare_sample(df);

    String ruleString = "set col: name value: if(length(name) > 5, '11', '10')";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_derive_mul() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "derive as: Turbo value: speed * 10";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doDerive((Derive)rule);
    newDf.show();
  }

  @Test
  public void test_set_div() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "set col: speed value: speed / 10";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_div_double() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "set col: speed value: 3.0 / speed";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSet((Set)rule);
    newDf.show();
  }

  @Test
  public void test_set_type_mismatch() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "set col: speed value: speed * '10'";
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

  private DataFrame prepare_product(DataFrame product) throws TeddyException {
    List<String> ruleStrings = new ArrayList<>();
    ruleStrings.add("rename col: column1 to: pcode1");
    ruleStrings.add("rename col: column2 to: pcode2");
    ruleStrings.add("rename col: column3 to: pcode3");
    ruleStrings.add("rename col: column4 to: pcode4");

    ruleStrings.add("settype col: pcode1 type: long");
    ruleStrings.add("settype col: pcode2 type: long");
    ruleStrings.add("settype col: pcode3 type: long");
    ruleStrings.add("settype col: pcode4 type: long");
    ruleStrings.add("rename col: column5 to: pcode");

    return apply_rule(product, ruleStrings);
  }

  private DataFrame prepare_store(DataFrame store) throws TeddyException {
    List<String> ruleStrings = new ArrayList<>();
    ruleStrings.add("header rownum: 1");
    ruleStrings.add("drop col: store_name");
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

    String ruleString = "join leftSelectCol: cdate,pcode1,pcode2,pcode3,pcode4,customer_id,detail_store_code rightSelectCol: detail_store_code,customer_id,detail_store_name condition: customer_id=customer_id joinType: 'inner' dataset2: '88888888-4444-4444-4444-121212121212'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doJoin((Join)rule, store, 10000);
    newDf.show();
  }

  @Test
  public void test_join_multi_key() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract.show();

    contract = prepare_contract(contract);
    contract.show();

    DataFrame product = new DataFrame();
    product.setGrid(grids.get("product"));
    product.show();

    product = prepare_product(product);
    product.show();

    String ruleString = "join leftSelectCol: cdate,pcode1,pcode2,pcode3,pcode4,customer_id,detail_store_code rightSelectCol: pcode1,pcode2,pcode3,pcode4,pcode condition: pcode1=pcode1 && pcode2=pcode2 && pcode3=pcode3 && pcode4=pcode4 joinType: 'inner' dataset2: '88888888-4444-4444-4444-121212121212'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doJoin((Join)rule, product, 10000);
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

    String ruleString = "join leftSelectCol: cdate,pcode1,pcode2,pcode3,pcode4,customer_id,detail_store_code rightSelectCol: detail_store_code,customer_id,detail_store_name condition: detail_store_code=detail_store_code joinType: 'inner' dataset2: '88888888-4444-4444-4444-121212121212'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doJoin((Join)rule, store, 10000);
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
    df = prepare_sample(df);
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
    contract.show();

    String ruleString = "extract col: cdate on: /\\w+/ quote: '\"' limit: 3";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doExtract((Extract)rule);
    newDf.show();
  }

  @Test
  public void test_countpattern() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "countpattern col: cdate on: '2'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doCountPattern((CountPattern) rule);
    newDf.show();
  }

  @Test
  public void test_countpattern_regex() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "countpattern col: cdate on: /0\\d+/";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doCountPattern((CountPattern) rule);
    newDf.show();
  }

  @Test
  public void test_countpattern_ignorecase() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "countpattern col: name on: 'm'";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doCountPattern((CountPattern) rule);
    newDf.show();

    ruleString = "countpattern col: name on: 'm' ignoreCase: true";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doCountPattern((CountPattern) rule);
    newDf.show();
  }

  @Test
  public void test_replace() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "replace col: cdate on: '0' with: 'X' global: false";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doReplace((Replace) rule);
    newDf.show();

    ruleString = "replace col: cdate on: '0' with: 'X' global: true";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = contract.doReplace((Replace) rule);
    newDf.show();
  }

  @Test
  public void test_replace_regex() throws IOException, TeddyException {
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
  public void test_nest_map() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "nest col: pcode1, pcode2, pcode3, pcode4 into: map as: pcode";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doNest((Nest)rule);
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

    ruleString = "unnest col: pcode into: map idx: 'pcode3'"; // into: is not used (remains for backward-compatability)
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doUnnest((Unnest)rule);
    newDf.show();
  }

  @Test
  public void test_flatten() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "nest col: pcode1, pcode2, pcode3, pcode4 into: array as: pcode";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doNest((Nest)rule);
    newDf.show();

    ruleString = "flatten col: pcode";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doFlatten((Flatten) rule);
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

  @Test
  public void test_split_ignorecase() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grids.get("sample"));
    df = prepare_sample(df);
    df.show();

    String ruleString = "split col: name on: 'm' limit: 2";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = df.doSplit((Split)rule);
    newDf.show();

    ruleString = "split col: name on: 'm' limit: 2 ignoreCase: true";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doSplit((Split)rule);
    newDf.show();
  }
  @Test
  public void test_aggregate_sum() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "aggregate value: 'sum(pcode4)' group: pcode1, pcode2";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doAggregate((Aggregate)rule);
    newDf.show();
  }

  @Test
  public void test_aggregate_count() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "aggregate value: 'count()' group: pcode1, pcode2";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doAggregate((Aggregate)rule);
    newDf.show();
  }

  @Test
  public void test_aggregate_avg() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "aggregate value: 'avg(pcode4)' group: pcode1, pcode2";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doAggregate((Aggregate)rule);
    newDf.show();
  }

  @Test
  public void test_aggregate_min_max() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "aggregate value: 'min(detail_store_code)', 'max(detail_store_code)' group: pcode1, pcode2";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doAggregate((Aggregate)rule);
    newDf.show();
  }

  @Test
  public void test_sort() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "sort order: detail_store_code";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doSort((Sort)rule);
    newDf.show(100);
  }

  @Test
  public void test_sort_multi() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "sort order: pcode1, pcode2, pcode3";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doSort((Sort)rule);
    newDf.show(1000);
  }

  @Test
  public void test_pivot_sum() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "pivot col: pcode1, pcode2 value: 'sum(detail_store_code)', 'count()' group: pcode3, pcode4";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doPivot((Pivot) rule);
    newDf.show();
  }

  @Test
  public void test_pivot_avg() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "pivot col: pcode1, pcode2 value: 'avg(detail_store_code)' group: pcode3, pcode4";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doPivot((Pivot) rule);
    newDf.show(100);
  }

  @Test
  public void test_unpivot_sum() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "pivot col: pcode1 value: 'sum(detail_store_code)' group: pcode3, pcode4";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doPivot((Pivot) rule);

    ruleString = "unpivot col: sum_detail_store_code_1, sum_detail_store_code_2, sum_detail_store_code_3, sum_detail_store_code_4 groupEvery: 1";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doUnpivot((Unpivot) rule);

    newDf.show();
  }

  @Test
  public void test_unpivot_sum_every() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "pivot col: pcode1 value: 'sum(detail_store_code)' group: pcode3, pcode4";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doPivot((Pivot) rule);

    ruleString = "unpivot col: sum_detail_store_code_1, sum_detail_store_code_2, sum_detail_store_code_3, sum_detail_store_code_4 groupEvery: 4";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doUnpivot((Unpivot) rule);

    newDf.show();
  }

  @Test
  public void test_keep() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "keep row: if(pcode4 < 10)";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doKeep((Keep) rule);

    newDf.show();
  }

  @Test
  public void test_keep_literal() throws IOException, TeddyException {
    DataFrame store = new DataFrame();
    store.setGrid(grids.get("store"));
    store = prepare_store(store);
    store.show();

    String ruleString = "keep row: if(store_code=='0001')";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = store.doKeep((Keep) rule);

    newDf.show();
  }

  @Test
  public void test_delete() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "delete row: if(pcode4 < 10)";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doDelete((Delete) rule);

    newDf.show();
  }

  @Test
  public void test_move_before() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "move col: pcode4 before: pcode1";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doMove((Move) rule);
    newDf.show();

    ruleString = "move col: pcode4 before: cdate";
    rule = new RuleVisitorParser().parse(ruleString);
    newDf = newDf.doMove((Move) rule);
    newDf.show();
  }

  @Test
  public void test_move_after() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "move col: pcode4 after: customer_id";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doMove((Move) rule);
    newDf.show();
  }

  @Test
  public void test_move_after_last() throws IOException, TeddyException {
    DataFrame contract = new DataFrame();
    contract.setGrid(grids.get("contract"));
    contract = prepare_contract(contract);
    contract.show();

    String ruleString = "move col: pcode4 after: detail_store_code";
    Rule rule = new RuleVisitorParser().parse(ruleString);
    DataFrame newDf = contract.doMove((Move) rule);
    newDf.show();
  }
}
