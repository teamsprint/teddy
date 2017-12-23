import com.skt.metatron.teddy.DataFrame;
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
  public void test_rename() {
    DataFrame df = new DataFrame();
    df.setGrid(grid);
    df.show();

    DataFrame newDf = df.withColumnRenamed("column1", "newName");
    newDf.show();
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

}
