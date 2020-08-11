import com.qlangtech.tis.plugin.incr.TestDefaultIncrK8sConfig;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2020-08-11 11:10
 **/
public class TestAll extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestDefaultIncrK8sConfig.class);
        return suite;
    }
}
