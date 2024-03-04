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
    private static final Logger logger = LoggerFactory.getLogger(SampleApplication.class);

    public static void main(String[] args) {

        TIS.afterTISCreate = (tis) -> {
            logger.info("afterTISCreate", new Exception());
        };


        SpringApplication.run(SampleApplication.class, args);
    }
}
