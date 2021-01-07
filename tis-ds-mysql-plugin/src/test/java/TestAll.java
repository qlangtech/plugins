import com.qlangtech.tis.plugin.ds.mysql.TestMySQLDataSourceFactory;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2021-01-07 18:52
 **/
public class TestAll extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestMySQLDataSourceFactory.class);
        return suite;
    }
}
