import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.Derive;
import com.skt.metatron.discovery.common.preparation.rule.Rule;
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
 * Derive Test (in Transform)
 */
public class DeriveTest {

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
  public void testDerive1() throws IOException, TeddyException {
    DataFrame null_contained = newNullContainedDataFrame();

    Rule rule = new RuleVisitorParser().parse("derive value: itemNo as: 'cate_if'");
    DataFrame newDf = null_contained.doDerive((Derive) rule);
    newDf.show();
    assertEquals(new Long(1), newDf.objGrid.get(0).get("cate_if")); // 1
    assertEquals(null, newDf.objGrid.get(1).get("cate_if"));        // null
  }

  @Test
  public void testDerive2() throws IOException, TeddyException {
    DataFrame null_contained = new DataFrame();
    null_contained.setGrid(grids.get("null_contained"));
    null_contained = DataFrameTest.prepare_null_contained(null_contained);
    null_contained.show();

    Rule rule = new RuleVisitorParser().parse("derive value: if(itemNo) as: 'cate_if'");
    DataFrame newDf = null_contained.doDerive((Derive) rule);
    newDf.show();
    assertEquals(true, newDf.objGrid.get(0).get("cate_if"));    // 1
    assertEquals(false, newDf.objGrid.get(1).get("cate_if"));   // null
  }

  @Test
  public void testDerive3() throws IOException, TeddyException {
    DataFrame null_contained = new DataFrame();
    null_contained.setGrid(grids.get("null_contained"));
    null_contained = DataFrameTest.prepare_null_contained(null_contained);
    null_contained.show();

    Rule rule = new RuleVisitorParser().parse("derive value: if(isnull(itemNo), '1', '2') as: 'cate_if'");
    DataFrame newDf = null_contained.doDerive((Derive) rule);
    newDf.show();
    assertEquals("2", newDf.objGrid.get(0).get("cate_if"));   // 1
    assertEquals("1", newDf.objGrid.get(1).get("cate_if"));   // null
  }

  @Test
  public void testDerive4() throws IOException, TeddyException {
    DataFrame null_contained = new DataFrame();
    null_contained.setGrid(grids.get("null_contained"));
    null_contained = DataFrameTest.prepare_null_contained(null_contained);
    null_contained.show();

    Rule rule = new RuleVisitorParser().parse("derive value: if(itemNo, 1, 2) as: 'cate_if'");
    DataFrame newDf = null_contained.doDerive((Derive) rule);
    newDf.show();

    assertEquals(new Long(1), newDf.objGrid.get(0).get("cate_if"));   // 1
    assertEquals(new Long(2), newDf.objGrid.get(1).get("cate_if"));   // null
  }

  @Test
  public void testDerive5() throws IOException, TeddyException {
    DataFrame null_contained = new DataFrame();
    null_contained.setGrid(grids.get("null_contained"));
    null_contained = DataFrameTest.prepare_null_contained(null_contained);
    null_contained.show();

    Rule rule = new RuleVisitorParser().parse("derive value: if(itemNo, 1.0, 2.0) as: 'cate_if'");
    DataFrame newDf = null_contained.doDerive((Derive) rule);
    newDf.show();

    assertEquals(new Double(1.0), newDf.objGrid.get(0).get("cate_if"));   // 1
    assertEquals(new Double(2.0), newDf.objGrid.get(1).get("cate_if"));   // null
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

//
//    @Test
//    public void testDerive6() {
//        String rule = "derive value: if(itemNo == 5) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive7() {
//        String rule = "derive value: if(name == 'Ferrari') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive8() {
//        String rule = "derive value: if(name == 'Ferrari', '1', '0') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("1", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive9() {
//        String rule = "derive value: if(name == 'Ferrari', 1, 0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive10() {
//        String rule = "derive value: if(name == 'Ferrari', 10.0, 1.0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(10.0, resultDF.select("cate_if").as(Encoders.DOUBLE()).first());
//    }
//
//    @Test
//    public void testDerive11() {
//        String rule = "derive value: if(itemNo <= 3, 1, 0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive12() {
//        String rule = "derive value: if(weight > 1000, 'heavy', 'light') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("light", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive13() {
//        String rule = "derive value: if(itemNo < 3) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive14() {
//        String rule = "derive value: if(speed > 300 && speed < 400) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive15() {
//        String rule = "derive value: if(speed > 300 && speed < 400 || weight < 1000) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive16() {
//        String rule = "derive value: if(speed > 300 && speed < 400 && weight < 1000) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive17() {
//        String rule = "derive value: if(speed > 300 && speed < 400 && weight < 1000, 'good', 'bad') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("bad", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive18() {
//        String rule = "derive value: if(speed > 300 && speed < 400 && weight < 1000, 1, 0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(0), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive19() {
//        String rule = "derive value: if(speed > 300 && speed < 400 && weight < 1000, 10.0, 1.0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(1.0, resultDF.select("cate_if").as(Encoders.DOUBLE()).first());
//    }
//
//    @Test
//    public void testDerive20() {
//        String rule = "derive value: upper(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("FERRARI", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive21() {
//        String rule = "derive value: isnull(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive22() {
//        String rule = "derive value: length(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(7), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive23() {
//        String rule = "derive value: if(length(name)) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive24() {
//        String rule = "derive value: if(length(name) > 5, '1', '0') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("1", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive25() {
//        String rule = "derive value: if(length(name) < 7, 1, 0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(0), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive26() {
//        String rule = "derive value: if(length(name) < 7, 10.0, 1.0) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(1.0, resultDF.select("cate_if").as(Encoders.DOUBLE()).first());
//    }
//
//    @Test
//    public void testDerive27() {
//        String rule = "derive value: if(length(name) == 4, '4c', 'others') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("others", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive28() {
//        String rule = "derive value: weight + 100 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(900), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive29() {
//        String rule = "derive value: weight + 100.78 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(900.78, resultDF.select("cate_if").as(Encoders.DOUBLE()).first());
//    }
//
//    @Test
//    public void testDerive30() {
//        String rule = "derive value: weight - 100 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(700), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive31() {
//        String rule = "derive value: weight * 100 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(80000), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive32() {
//        String rule = "derive value: weight / 100 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(8.0, resultDF.select("cate_if").as(Encoders.DOUBLE()).first());
//    }
//
//    @Test
//    public void testDerive33() {
//        String rule = "derive value: speed + weight as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1059), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive34() {
//        String rule = "derive value: weight + speed + itemNo as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1060), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive35() {
//        String rule = "derive value: speed + 100 - weight + 2 - 3 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(-442), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive36() {
//        String rule = "derive value: length(name) + speed + itemNo + 100 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(367), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive37() {
//        String rule = "derive value: math.sqrt(speed) + math.sqrt(weight) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        String result = String.valueOf(resultDF.select("cate_if").as(Encoders.STRING()).first()).substring(0,6);
//        assertEquals("44.377", result);
//    }
//
//    @Test
//    public void testDerive38() {
//        String rule = "derive value: 5 + weight as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(805), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive39() {
////        String rule = "derive value: if(floor(datediff(to_date(contract_date), to_date(birth_date))/365.25/10) == 1, 1, 0) as: age_10";
//        String rule = "derive value: math.floor(datediff (to_date(contract_date, 'yyyy-MM-dd'), to_date(birth_date, 'yyyy-MM-dd'))) as: age_10";
////        runAndPrint(rule);
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(2557), resultDF.select("age_10").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive40() {
//        String rule = "derive value: 1 as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive41() {
//        String rule = "derive value: substring(name, 1, 2) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("Fe", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive42() {
//        String rule = "derive value: if(substring(name, 1, 2) == 'Fe') as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive43() {
//        String rule = "derive value: if(substring(name, 1, 2)) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive44() {
//        String rule = "derive value: if(substring(name, 1, 2), 1, 2) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(new Long(1), resultDF.select("cate_if").as(Encoders.LONG()).first());
//    }
//
//    @Test
//    public void testDerive45() {
//        String rule = "derive value: if('Ferrari' == name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(true, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive46() {
//        String rule = "derive value: if(length(name) + 1 == length(name) + 2) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive47() {
//        String rule = "derive value: isnull(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals(false, resultDF.select("cate_if").as(Encoders.BOOLEAN()).first());
//    }
//
//    @Test
//    public void testDerive48() {
//        String rule = "derive value: upper(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("FERRARI", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive49() {
//        String rule = "derive value: lower(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("ferrari", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive50() {
//        String rule = "derive value: trim(name) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("Ferrari", resultDF.select("cate_if").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive51() {
//        String rule = "derive value: math.pow(speed, 3) as: 'cate_if'";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        String result = String.valueOf(resultDF.select("cate_if").as(Encoders.STRING()).first()).substring(0,6);
//        assertEquals("1.7373", result);
//    }
//
////    @Test
////    public void testSe52() {
////        String rule = "derive value: if(name == 'Ferrari', '1') as: 'cate_if'";
////        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
////        resultDF.show();
////        assertEquals("1", resultDF.select("cate_if").as(Encoders.STRING()).first());
////    }
////
////    @Test
////    public void testSe53() {
////        String rule = "derive value: if(name == 'Ferrari', 1) as: 'cate_if'";
////        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
////        resultDF.show();
////        assertEquals("1", resultDF.select("cate_if").as(Encoders.STRING()).first());
////    }
////
////    @Test
////    public void testSe54() {
////        String rule = "derive value: if(name == 'Ferrari', 1.0) as: 'cate_if'";
////        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
////        resultDF.show();
////        assertEquals("1.0", resultDF.select("cate_if").as(Encoders.STRING()).first());
////    }
//
//    @Test
//    public void testDerive55() {
//        String rule = "derive value: if(name == 'Ferrari', itemNo, speed) as: 'name' ";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("1", resultDF.select("name_1").as(Encoders.STRING()).first());
//    }
//
//    @Test
//    public void testDerive56() {
//        String rule = "derive value: if(speed>=300, 'Yes', 'No') as: 'name' ";
//        Dataset resultDF = metisService.transform(getRuleSet(rule), true).getResultSet();
//        resultDF.show();
//        assertEquals("No", resultDF.select("name_1").as(Encoders.STRING()).first());
//    }
//
//    private void runAndPrint(String ruleCode) {
//        Rule rule = new RuleVisitorParser().parse(ruleCode);
//
//        ObjectMapper mapper = new ObjectMapper();
//        String json = null;
//        try {
//            json = mapper.writeValueAsString(rule);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//        System.out.printf("code below: %n '%s' %n has been parsed to object: %n '%s'%n '%s'%n", ruleCode, json, rule.toString());
//    }

}
