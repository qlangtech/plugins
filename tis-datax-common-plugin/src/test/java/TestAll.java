import com.qlangtech.tis.plugin.datax.TestDataXGlobalConfig;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2021-04-21 11:31
 **/
public class TestAll {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestDataXGlobalConfig.class);
        return suite;
    }

}
