import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.*;
import com.skt.metatron.teddy.DataFrame;
import com.skt.metatron.teddy.TeddyException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
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

  static private List<String[]> grid;

  @BeforeClass
  public static void setUp() throws Exception {
    grid = new ArrayList<>();

    BufferedReader br = null;
    String line;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(getResourcePath("metatron_dataset/small/sample.csv"))));
      while ((line = br.readLine()) != null) {
        String[] strCols = line.split(cvsSplitBy);
        System.out.println(strCols);
        grid.add(strCols);
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
    df.setGrid(grid);
    df.show();
  }

  @Test
  public void test_drop() {
    DataFrame df = new DataFrame();
    df.setGrid(grid);
    df.show();

    List<String> targetColNames = new ArrayList<>();
    targetColNames.add("column1");
    targetColNames.add("column3");
    targetColNames.add("column6");
    DataFrame newDf = df.drop(targetColNames);
    newDf.show();
  }

  @Test
  public void test_select() {
    DataFrame df = new DataFrame();
    df.setGrid(grid);
    df.show();

    List<String> targetColNames = new ArrayList<>();
    targetColNames.add("column4");
    targetColNames.add("column5");
    DataFrame newDf = df.select(targetColNames);
    newDf.show();
  }

  private DataFrame prepare_common(DataFrame df) throws IOException, TeddyException {
    // rename column5 -> HP
    Rule rule = new RuleVisitorParser().parse("rename col: column5 to: HP");
    df = df.doRename((Rename)rule);

    rule = new RuleVisitorParser().parse("settype col: HP type: long");
    return df.doSetType((SetType)rule);
  }

  @Test
  public void test_rename_settype() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grid);
    df.show();
    df = prepare_common(df);
    df.show();
  }

  @Test
  public void test_set_plus() throws IOException, TeddyException {
    DataFrame df = new DataFrame();
    df.setGrid(grid);
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
    df.setGrid(grid);
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
    df.setGrid(grid);
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
    df.setGrid(grid);
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
    df.setGrid(grid);
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
    df.setGrid(grid);
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
}
