import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.Rule;
import com.skt.metatron.discovery.common.preparation.rule.Set;
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

import static org.junit.Assert.assertEquals;

/**
 * WrangleTest
 */
public class SetTest {

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
    loadGridCsv("null_contained", "metatron_dataset/small/null_contained.csv");
  }

  private DataFrame newNullContainedDataFrame() throws IOException, TeddyException {
    DataFrame null_contained = new DataFrame();
    null_contained.setGrid(grids.get("null_contained"));
    null_contained = DataFrameTest.prepare_null_contained(null_contained);
    null_contained.show();
    return null_contained;
  }

  @Test
  public void testSet1() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(itemNo, 'a', 'b')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("a", newDf.objGrid.get(0).get("name"));   // 1
    assertEquals("b", newDf.objGrid.get(1).get("name"));   // null
  }

  @Test
  public void testSet2() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true,  newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals(false, newDf.objGrid.get(1).get("itemNo"));   // null
  }

  @Test
  public void testSet3() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo, '1', '2')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("1", newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals("2", newDf.objGrid.get(1).get("itemNo"));   // null
  }

  @Test
  public void testSet4() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo, 1, 2)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1), newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals(new Long(2), newDf.objGrid.get(1).get("itemNo"));   // null
  }

  @Test
  public void testSet5() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo, 1.0, 2.0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(1.0, newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals(2.0, newDf.objGrid.get(1).get("itemNo"));   // null
  }

  @Test
  public void testSet6() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo == 5)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(false, newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals(false, newDf.objGrid.get(1).get("itemNo"));   // null
    assertEquals(true,  newDf.objGrid.get(4).get("itemNo"));   // 5
  }

  @Test
  public void testSet7() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true,  newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(false, newDf.objGrid.get(1).get("name"));   // Jaguar
    assertEquals(false, newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet8() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari', '1', '0')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("1", newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals("0", newDf.objGrid.get(1).get("name"));   // Jaguar
    assertEquals("0", newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet9() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari', 1, 0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1), newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(new Long(0), newDf.objGrid.get(1).get("name"));   // Jaguar
    assertEquals(new Long(0), newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet10() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari', 10.0, 1.0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(10.0, newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(1.0,  newDf.objGrid.get(1).get("name"));   // Jaguar
    assertEquals(1.0,  newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet11() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo <= 3, 1, 0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1), newDf.objGrid.get(0).get("itemNo"));   // Ferrari
    assertEquals(new Long(0), newDf.objGrid.get(4).get("itemNo"));   // Lamborghini
    assertEquals(new Long(0), newDf.objGrid.get(5).get("itemNo"));   // null
  }

  @Test
  public void testSet12() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: if(weight > 1000, 'heavy', 'light')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("light", newDf.objGrid.get(0).get("weight"));   // 800
    assertEquals("heavy", newDf.objGrid.get(2).get("weight"));   // 1800
  }

  // original dataset
  // +----------+-------------+------+-----------+-----+------+
  // |birth_date|contract_date|itemNo|       name|speed|weight|
  // +----------+-------------+------+-----------+-----+------+
  // |2010-01-01|   2017-01-01|     1|    Ferrari|  259|   800|
  // |2000-01-01|   2017-01-01|  null|     Jaguar|  274|   998|
  // |1990-01-01|   2017-01-01|     3|   Mercedes|  340|  1800|
  // |1980-01-01|   2017-01-01|     4|       Audi|  345|   875|
  // |1970-01-01|   2017-01-01|     5|Lamborghini|  355|  1490|
  // |1970-01-01|   2017-01-01|     6|       null| null|  1490|
  // +----------+-------------+------+-----------+-----+------+

  @Test
  public void testSet13() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo < 3)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true,  newDf.objGrid.get(0).get("itemNo"));   // 1
    assertEquals(false, newDf.objGrid.get(3).get("itemNo"));   // 4
    assertEquals(false, newDf.objGrid.get(5).get("itemNo"));   // null
  }

  @Test
  public void testSet14() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: if(speed > 300 && speed < 400 && speed != 350)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(false, newDf.objGrid.get(0).get("speed"));   // 259
    assertEquals(true,  newDf.objGrid.get(2).get("speed"));   // 340
    assertEquals(false, newDf.objGrid.get(5).get("speed"));   // null
  }

  @Test
  public void testSet15() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed  value: if(speed > 300 && speed < 400 || weight < 1000)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true,  newDf.objGrid.get(0).get("speed"));   // 259, 800
    assertEquals(true,  newDf.objGrid.get(1).get("speed"));   // 274, 998
    assertEquals(true,  newDf.objGrid.get(2).get("speed"));   // 340, 1800
    assertEquals(false, newDf.objGrid.get(5).get("speed"));   // null, 1490
  }

  @Test
  public void testSet16() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: if(speed > 300 && speed < 400 && weight < 1000)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(false, newDf.objGrid.get(0).get("speed"));   // 259, 800
    assertEquals(false, newDf.objGrid.get(1).get("speed"));   // 274, 998
    assertEquals(true,  newDf.objGrid.get(3).get("speed"));   // 355, 1490
    assertEquals(false, newDf.objGrid.get(5).get("speed"));   // null, 1490
  }

  @Test
  public void testSet17() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: if(speed > 300 && speed < 400 && weight < 1000, 'good', 'bad')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("bad",  newDf.objGrid.get(0).get("speed"));   // 259, 800
    assertEquals("bad",  newDf.objGrid.get(1).get("speed"));   // 274, 998
    assertEquals("good", newDf.objGrid.get(3).get("speed"));   // 355, 1490
    assertEquals("bad",  newDf.objGrid.get(5).get("speed"));   // null, 1490
  }

  @Test
  public void testSet18() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: if(speed > 300 && speed < 400 && weight < 1000, 1, 0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(0),  newDf.objGrid.get(0).get("speed"));   // 259, 800
    assertEquals(new Long(0),  newDf.objGrid.get(1).get("speed"));   // 274, 998
    assertEquals(new Long(1), newDf.objGrid.get(3).get("speed"));    // 355, 1490
    assertEquals(new Long(0),  newDf.objGrid.get(5).get("speed"));   // null, 1490
  }

  @Test
  public void testSet19() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: if(speed > 300 && speed < 400 && weight < 1000, 10.0, 1.0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(1.0,  newDf.objGrid.get(0).get("speed"));   // 259, 800
    assertEquals(1.0,  newDf.objGrid.get(1).get("speed"));   // 274, 998
    assertEquals(10.0, newDf.objGrid.get(3).get("speed"));   // 355, 1490
    assertEquals(1.0,  newDf.objGrid.get(5).get("speed"));   // null, 1490
  }

  @Test
  public void testSet20() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: upper(name)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("FERRARI", newDf.objGrid.get(0).get("name"));
  }

  @Test
  public void testSet21() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: isnull(name)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(false, newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(true,  newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet22() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: length(name)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(7), newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(null,        newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet23() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(length(name))");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true,  newDf.objGrid.get(0).get("name"));   // Ferrari
    assertEquals(false, newDf.objGrid.get(5).get("name"));   // null
  }

  @Test
  public void testSet24() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(length(name) > 5, '1', '0')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("1", newDf.objGrid.get(0).get("name"));  // Ferrari
    assertEquals("0", newDf.objGrid.get(3).get("name"));  // Audi
    assertEquals("0", newDf.objGrid.get(5).get("name"));  // null
    // the conditional result of null is the false.
  }

  @Test
  public void testSet25() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(length(name) < 7, 1, 0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(0), newDf.objGrid.get(0).get("name"));  // Ferrari
    assertEquals(new Long(1), newDf.objGrid.get(3).get("name"));  // Audi
    assertEquals(new Long(0), newDf.objGrid.get(5).get("name"));  // null
    // the conditional result of null is the false.
  }

  @Test
  public void testSet26() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(length(name) < 7, 10.0, 1.0)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(1.0,  newDf.objGrid.get(0).get("name"));  // Ferrari
    assertEquals(10.0, newDf.objGrid.get(3).get("name"));  // Audi
    assertEquals(1.0,  newDf.objGrid.get(5).get("name"));  // null
    // the conditional result of null is the false.
  }

  @Test
  public void testSet27() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(length(name) == 4, '4c', 'others')");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("others", newDf.objGrid.get(0).get("name"));  // Ferrari
    assertEquals("4c",     newDf.objGrid.get(3).get("name"));  // Audi
    assertEquals("others", newDf.objGrid.get(5).get("name"));  // null
    // the conditional result of null is the false.
  }

  @Test
  public void testSet28() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight + 100");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(900), newDf.objGrid.get(0).get("weight"));  // 800
  }

  @Test
  public void testSet29() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight + 100.78");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(900.78, newDf.objGrid.get(0).get("weight"));  // 800
  }

  @Test
  public void testSet30() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight - 100");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(700), newDf.objGrid.get(0).get("weight"));  // 800
  }

  @Test
  public void testSet31() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight * 100");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(80000), newDf.objGrid.get(0).get("weight"));  // 800
  }

  @Test
  public void testSet32() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight / 100");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(8.0, newDf.objGrid.get(0).get("weight"));  // 800
  }

  @Test
  public void testSet33() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: speed + weight");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1059), newDf.objGrid.get(0).get("weight"));  // 259, 800
  }

  @Test
  public void testSet34() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: weight + speed + itemNo");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1060), newDf.objGrid.get(0).get("weight"));  // 259, 800, 1
  }

  @Test
  public void testSet35() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: speed + 100 - weight + 2 - 3");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(-442), newDf.objGrid.get(0).get("weight"));  // 259, 800
  }

  @Test
  public void testSet36() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: length(name) + speed + itemNo + 100");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(367), newDf.objGrid.get(0).get("name"));  // Ferrari, 259, 800
  }

  @Test
  public void testSet37() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: math.sqrt(speed) + math.sqrt(weight)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("44.377", newDf.objGrid.get(0).get("name").toString().substring(0, 6));  // 259, 800
  }

  @Test
  public void testSet38() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: weight value: 5 + weight");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(805), newDf.objGrid.get(0).get("weight"));  // 800
  }

//  @Test
//  public void testSet39() throws IOException, TeddyException {
//    DataFrame null_contained = newNullContainedDataFrame();
//    Rule rule = new RuleVisitorParser().parse("set col: contract_date value: math.floor(datediff (to_date(contract_date, 'yyyy-MM-dd'), to_date(birth_date, 'yyyy-MM-dd'))/365.25/10)");
//    DataFrame newDf = null_contained.doSet((Set) rule);
//    newDf.show();
//
//    assertEquals(new Long(805), newDf.objGrid.get(0).get("weight"));  // 800
//  }

  @Test
  public void testSet40() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(itemNo) row: itemNo > 2");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(true, newDf.objGrid.get(0).get("itemNo"));  // 1
  }

  // testSet41 deleted for row clause in set rule has been deprecated
  // testSet42 deleted for row clause in set rule has been deprecated
  // testSet43 deleted for row clause in set rule has been deprecated
  // testSet44 deleted for row clause in set rule has been deprecated
  // testSet45 deleted for row clause in set rule has been deprecated
  // testSet46 deleted for row clause in set rule has been deprecated
  // testSet47 deleted for row clause in set rule has been deprecated
  // testSet48 deleted for row clause in set rule has been deprecated
  // testSet49 deleted for row clause in set rule has been deprecated
  // testSet50 deleted for row clause in set rule has been deprecated

  @Test
  public void testSet51() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: speed value: '001'");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("001", newDf.objGrid.get(0).get("speed"));  // 259
  }

  @Test
  public void testSet52() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: length(upper(name))");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(7), newDf.objGrid.get(0).get("name"));  // Ferrari
  }

  // testSet53 deleted for 2-argument conditional expressions are not supported any more.
  // testSet54 deleted for 2-argument conditional expressions are not supported any more.
  // testSet55 deleted for 2-argument conditional expressions are not supported any more.

  @Test
  public void testSet56() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari', itemNo, 3)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(1), newDf.objGrid.get(0).get("name"));  // Ferrari
  }

  @Test
  public void testSet57() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: name value: if(name == 'Ferrari', 'Ferrari', name)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals("Ferrari", newDf.objGrid.get(0).get("name"));  // Ferrari
    assertEquals("Jaguar",  newDf.objGrid.get(1).get("name"));  // Jaguar
  }

//  @Test
//  public void testSet58() throws IOException, TeddyException {
//    DataFrame null_contained = newNullContainedDataFrame();
//    Rule rule = new RuleVisitorParser().parse("set col: name value: coalesce(speed, weight)");
//    DataFrame newDf = null_contained.doSet((Set) rule);
//    newDf.show();
//
//    assertEquals(new Long(259),  newDf.objGrid.get(0).get("name"));  // 259, 800
//    assertEquals(new Long(1490), newDf.objGrid.get(5).get("name"));  // null, 1490
//  }
//
//    @Test
//    public void testSet58() {
//        String rule = "set col: name value: coalesce(speed, weight)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(259), resultDF.select("name").as(Encoders.LONG()).first());
//    }

  @Test
  public void testSet59() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();
    Rule rule = new RuleVisitorParser().parse("set col: itemNo value: if(isnull(speed), null, weight)");
    DataFrame newDf = null_contained.doSet((Set) rule);
    newDf.show();

    assertEquals(new Long(800), newDf.objGrid.get(0).get("itemNo"));  // Ferrari
    assertEquals(null,          newDf.objGrid.get(5).get("itemNo"));  // null
  }

//    @Test
//    public void testSet59() {
//        String rule = "set col: name value: if(isnull(speed), null, weight)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(800), resultDF.select("name").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testSet60() {
//        String rule = "set col: name value: if(name == 'Ferrari', true, false)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("name").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testSet61() {
//        String rule = "set col: name value: if(name == 'Ferrari', true)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("name").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testSet62() {
//        String rule = "set col: name value: if(name == 'Ferrari', null, name)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(null, resultDF.select("name").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testSet63() {
//        String rule = "set col: speed value: if(isnull(speed), -1, speed)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(259), resultDF.select("speed").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testSet64() {
//        String rule = "set col: speed value: if(speed>=6, 'Yes', 'No')";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("Yes", resultDF.select("speed").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testSet65() {
//        String rule = "set col: speed value: concat_ws('|', birth_date, itemNo, weight, name)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("2010-01-01|1|800|Ferrari", resultDF.select("speed").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testSet66() {
//        String rule = "set col: speed value: concat(birth_date, itemNo, weight, name)";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("2010-01-011800Ferrari", resultDF.select("speed").as(Encoders.STRING()).first());
//    }

    //String rule = "set col: speed value: speed + 2 * 1 > weight row: itemNo > 2";
    //String rule = "set col: speed value: speed + 2 * 1 row: itemNo > 2";
    //String rule = "set col: speed value: '1'"
}
