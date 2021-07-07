package parauto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

@Testcontainers
public abstract class MariaDBTestBase {
    private Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract MariaDBContainer getMariaDB();

    protected void logMonthVisitors(String title) throws SQLException {
        logger.info("============== month_visitor_list : {}", title);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select * from month_visitor")) {
            while (rs.next()) {
                logger.info("row: id={}, ym={}, loginId={}", rs.getLong("id"), rs.getInt("ym"), rs.getString("loginId"));
            }
        }
        logger.info("============== end");
    }

    protected void insertMonthVisitor(Connection conn, int ym, String loginId) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("insert into month_visitor (ym, loginid) values (?, ?)")) {
            pstmt.setLong(1, ym);
            pstmt.setString(2, loginId);
            pstmt.executeUpdate();
        }
    }

    private void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
            }
        }
    }

    private void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException throwables) {
            }
        }
    }

    protected long selectAutoInc() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT AUTO_INCREMENT from information_schema.`TABLES` WHERE TABLE_NAME='month_visitor'")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mariadb://localhost:" + getMariaDB().getMappedPort(3306) + "/test",
                getMariaDB().getUsername(),
                getMariaDB().getPassword());
    }

    protected void runAsync(Runnable run) {
        new Thread(run).start();
    }

    protected Result runWithTx(RunnableWithConnection runnable) {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            runnable.run(conn);
            conn.commit();
            return Result.success();
        } catch (Exception throwables) {
            rollback(conn);
            logger.info("fail to run: {}", throwables.getMessage());
            return Result.errorResult(throwables);
        } finally {
            close(conn);
        }
    }

    interface RunnableWithConnection {
        void run(Connection conn) throws Exception;
    }

    static class Result {
        Exception error;

        public Result(Exception error) {
            this.error = error;
        }

        public boolean isSuccess() {
            return error == null;
        }

        public boolean isError() {
            return error != null;
        }

        public String getErrorMessage() {
            return error == null ? null : error.getMessage();
        }

        static Result errorResult(Exception e) {
            return new Result(e);
        }

        static Result success() {
            return new Result(null);
        }
    }
}
