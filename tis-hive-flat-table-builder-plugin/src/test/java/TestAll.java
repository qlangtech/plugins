import com.qlangtech.tis.dump.hive.TestHiveDBUtils;
import com.qlangtech.tis.fullbuild.indexbuild.impl.TestYarnTableDumpFactory;
import com.qlangtech.tis.hive.TestHiveInsertFromSelectParser;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2020-06-03 14:09
 **/
public class TestAll extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestHiveInsertFromSelectParser.class);
        suite.addTestSuite(TestHiveDBUtils.class);
        suite.addTestSuite(TestYarnTableDumpFactory.class);
        return suite;
    }
}
