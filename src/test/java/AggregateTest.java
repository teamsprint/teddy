import com.skt.metatron.discovery.common.preparation.RuleVisitorParser;
import com.skt.metatron.discovery.common.preparation.rule.Aggregate;
import com.skt.metatron.discovery.common.preparation.rule.Rule;
import com.skt.metatron.discovery.common.preparation.rule.Sort;
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
public class AggregateTest {

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

    @Test
    public void test_aggregate1() throws IOException, TeddyException {
        DataFrame multi = new DataFrame();
        multi.setGrid(grids.get("multi"));
        multi = DataFrameTest.prepare_multi(multi);
        multi.show();

        String ruleString = "aggregate value: 'count()','sum(measure)', 'avg(measure)' group: machine_code,module_code";
        Rule rule = new RuleVisitorParser().parse(ruleString);
        DataFrame newDf = multi.doAggregate((Aggregate) rule);
        newDf.show();

        ruleString = "sort order: module_code";
        rule = new RuleVisitorParser().parse(ruleString);
        newDf = newDf.doSort((Sort) rule);
        newDf.show();

        assertEquals(new Long(36), newDf.rows.get(0).get("sum_measure"));
    }

    @Test
    public void test_aggregate2() throws TeddyException {
        DataFrame multi = new DataFrame();
        multi.setGrid(grids.get("multi"));
        multi = DataFrameTest.prepare_multi(multi);
        multi.show();

        String ruleString = "aggregate value: 'count()','sum(measure)', 'avg(measure)' group: machine_code,module_code,measure";
        Rule rule = new RuleVisitorParser().parse(ruleString);
        DataFrame newDf = multi.doAggregate((Aggregate) rule);
        newDf.show();

        ruleString = "sort order: count, module_code";
        rule = new RuleVisitorParser().parse(ruleString);
        newDf = newDf.doSort((Sort) rule);
        newDf.show();

        assertEquals(new Long(30), newDf.rows.get(0).get("sum_measure"));
    }

    @Test
    public void test_aggregate3() throws TeddyException {
        DataFrame multi = new DataFrame();
        multi.setGrid(grids.get("multi"));
        multi = DataFrameTest.prepare_multi(multi);
        multi.show();

        String ruleString = "aggregate value: 'count()','sum(measure)', 'avg(measure)'";
        Rule rule = new RuleVisitorParser().parse(ruleString);
        DataFrame newDf = multi.doAggregate((Aggregate) rule);
        newDf.show();

        assertEquals(new Long(205), newDf.rows.get(0).get("sum_measure"));
    }
}