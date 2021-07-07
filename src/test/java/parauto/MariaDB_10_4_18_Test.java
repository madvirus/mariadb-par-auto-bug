package parauto;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

@Testcontainers
public class MariaDB_10_4_18_Test extends MariaDBTestBase {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.4.18")
            .withConfigurationOverride("conf.d")
            .withInitScript("init.sql");
    
    @Test
    void test10_4_18() throws SQLException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        runAsync(() -> {
            runWithTx(conn -> {
                insertMonthVisitor(conn, 202107, "id1");
                Thread.sleep(2000);
                insertMonthVisitor(conn, 202107, "id2");
            });
            latch.countDown();
        });
        runAsync(() -> {
            runWithTx(conn -> {
                Thread.sleep(1000);
                insertMonthVisitor(conn, 202107, "id1");  // unique 에러
            });
            latch.countDown();
        });

        latch.await();

        logMonthVisitors("실행1");

        logger.info("next AUTO_INCREMENT: {}", selectAutoInc());

        runWithTx(conn -> {
            insertMonthVisitor(conn, 202107, "id4");
        });

        logMonthVisitors("id4 추가후");

        logger.info("next AUTO_INCREMENT: {}", selectAutoInc());

        Result result = runWithTx(conn -> {
            insertMonthVisitor(conn, 202107, "id5");
        });
        logMonthVisitors("id5 추가 후");
        logger.info("next AUTO_INCREMENT: {}", selectAutoInc());
    }

    @Override
    protected MariaDBContainer getMariaDB() {
        return mariaDB;
    }
}
