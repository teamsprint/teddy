import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.Pivot;
import com.skt.metatron.discovery.common.preparation.rule.Rule;
import com.skt.metatron.discovery.common.preparation.rule.Unpivot;
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
public class PivotTest {

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
    loadGridCsv("multi", "dataprep/pivot_test_multiple_column.csv");
  }

  private DataFrame newMultiDataFrame() throws IOException, TeddyException {
    DataFrame multi = new DataFrame();
    multi.setGrid(grids.get("multi"));
    multi = DataFrameTest.prepare_multi(multi);
    multi.show();
    return multi;
  }

  @Test
  public void test_pivot1() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute value: 'sum(measure)' group: machine_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(124), newDf.objGrid.get(0).get("sum_measure_00_00"));
  }

  @Test
  public void test_pivot2() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute value: 'sum(measure)' group: machine_code,module_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(30), newDf.objGrid.get(0).get("sum_measure_00_00"));
  }

  @Test
  public void test_pivot3() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column15 value: 'count()','sum(measure)' group: machine_code,module_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(30), newDf.objGrid.get(0).get("sum_measure_00_00_true"));
  }

  @Test
  public void test_pivot4() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column15 value: 'sum(measure)','count()' group: machine_code,module_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(30), newDf.objGrid.get(0).get("sum_measure_00_00_true"));
  }

  @Test
  public void test_pivot5() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column15 value: 'sum(measure)','min(measure)','max(measure)','count()' group: machine_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(32), newDf.objGrid.get(0).get("sum_measure_00_00_false"));
  }

  @Test
  public void test_pivot6() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column15 value: 'sum(measure)','count()' group: machine_code,module_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(0), newDf.objGrid.get(0).get("sum_measure_00_00_false"));
  }

  @Test
  public void test_pivot7() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column15,column7 value: 'sum(measure)','count()' group: machine_code,module_code");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(0), newDf.objGrid.get(0).get("sum_measure_00_00_false_3"));
  }

  @Test
  public void test_pivot8() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("pivot col: minute,column11 value: 'count()'");
    DataFrame newDf = multi.doPivot((Pivot) rule);
    newDf.show();

    assertEquals(new Long(8), newDf.objGrid.get(0).get("row_count_00_00_0_0"));
  }

  @Test
  public void test_unpivot1() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("unpivot col: minute,column15,column7 groupEvery: 3");
    DataFrame newDf = multi.doUnpivot((Unpivot) rule);
    newDf.show();

    assertEquals("00:00", newDf.objGrid.get(0).get("value1"));
  }

  @Test
  public void test_unpivot2() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("unpivot col: column7,column11,column16,column12 groupEvery: 1");
    DataFrame newDf = multi.doUnpivot((Unpivot) rule);
    newDf.show();

    assertEquals("2", newDf.objGrid.get(0).get("value1"));
  }

  @Test
  public void test_unpivot3() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("unpivot col: column7 groupEvery: 1");
    DataFrame newDf = multi.doUnpivot((Unpivot) rule);
    newDf.show();

    assertEquals("2", newDf.objGrid.get(0).get("value1"));
  }

  @Test
  public void test_unpivot4() throws IOException, TeddyException {
    DataFrame multi = newMultiDataFrame();
    Rule rule = new RuleVisitorParser().parse("unpivot col: column7");
    DataFrame newDf = multi.doUnpivot((Unpivot) rule);
    newDf.show();

    assertEquals("2", newDf.objGrid.get(0).get("value1"));
  }
}
