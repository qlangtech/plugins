import com.qlangtech.tis.TIS;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.offline.DataxUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import tech.powerjob.samples.SampleApplication;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/13
 */
@EnableScheduling
@SpringBootApplication
public class TestLauncher {
    public static void main(String[] args) {
        Config.setDataDir("/tmp/data");
       // TisAppLaunch.setTest(true);
        System.out.println("currentTimeStamp:" + DataxUtils.currentTimeStamp());
        TIS.permitInitialize = false;
        SpringApplication.run(SampleApplication.class, args);
    }
}
