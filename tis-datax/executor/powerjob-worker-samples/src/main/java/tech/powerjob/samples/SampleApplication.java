package tech.powerjob.samples;

import com.qlangtech.tis.TIS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 主类
 *
 * @author tjq
 * @since 2020/4/17
 */
@EnableScheduling
@SpringBootApplication
public class SampleApplication {
    public static void main(String[] args) {
        TIS.permitInitialize = false;
        SpringApplication.run(SampleApplication.class, args);
    }
}
