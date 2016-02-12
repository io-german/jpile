package com.opower.persistence.jpile.infile;

import com.google.common.collect.ImmutableList;
import com.opower.persistence.jpile.infile.driver.C3P0JdbcDriverSupport;
import com.opower.persistence.jpile.infile.driver.HikariJdbcDriverSupport;
import com.opower.persistence.jpile.infile.driver.MysqlJdbcDriverSupport;
import com.opower.persistence.jpile.jdbc.StatementCallback;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic Spring callback for executing the 'LOAD DATA INFILE' pattern of streaming data in
 * batch to MySQL. This will not work on databases other than MySQL. It requires simply
 * an SQL statement to execute, and an input stream from which to read. Since infile loads
 * do not stop for exceptional inserts necessarily, the driver collects all of the issues in
 * a {@link SQLWarning}. Since these statements do not return anything else that is meaningful, we
 * return this warning object, or null if there were no issues.
 * <p/>
 * This class depends not only on the MySQL Connector-J driver, but also on the C3P0 connection pool.
 * The latter wraps all statements in a proxy, so when using the connection pool you must use its API
 * to access the underlying MySQL statement. This class hides all of this tomfoolery behind a very
 * simple facade.
 * <p/>
 * Instances of this class are safe for use by multiple threads.
 *
 * @author s-m
 * @see <a href="http://dev.mysql.com/doc/refman/5.1/en/load-data.html">LOAD DATA INFILE reference</a>
 * @see com.mysql.jdbc.Statement#setLocalInfileInputStream(java.io.InputStream)
 * @since 1.0
 */
public class InfileStatementCallback implements StatementCallback<List<Exception>> {

    private static final List<JdbcDriverSupport> SUPPORTED_DRIVERS =
            ImmutableList.of(new HikariJdbcDriverSupport(), new C3P0JdbcDriverSupport(), new MysqlJdbcDriverSupport());

    // SQL statement
    private String loadInfileSql;
    // Source of data.
    private InputStream inputStream;

    /**
     * Constructs a callback from a SQL statement and a data stream from which to read.
     *
     * @param loadInfileSql to execute
     * @param inputStream   from which to read
     */
    public InfileStatementCallback(String loadInfileSql, InputStream inputStream) {
        this.loadInfileSql = loadInfileSql;
        this.inputStream = inputStream;
    }

    @Override
    public List<Exception> doInStatement(Statement statement) throws SQLException {
        for (JdbcDriverSupport support : SUPPORTED_DRIVERS) {
            if (support.accept(statement)) {
                support.doWithStatement(statement, this.inputStream);
                statement.execute(this.loadInfileSql);
                return extractWarnings(statement.getWarnings());
            }
        }
        throw new RuntimeException(String.format("Statement of type [%s] is not supported.", statement.getClass().getName()));
    }

    /**
     * Adds all of the warnings in the chain of a passed warning to a collection.
     *
     * @param warning result from a bulk load operation
     * @return list of warnings
     */
    private List<Exception> extractWarnings(SQLWarning warning) {
        List<Exception> warnings = new ArrayList<>(1000);
        while (warning != null) {
            warnings.add(warning);
            warning = warning.getNextWarning();
        }

        return warnings;
    }

    /**
     * Using this interface we can add new drivers
     */
    public interface JdbcDriverSupport {
        /**
         * Should return true if this is the right driver for this statement
         *
         * @param statement the statement to test
         * @return if it can handle this statement driver type
         */
        boolean accept(Statement statement);

        /**
         * Sets the input stream correctly on the statement
         *
         * @param statement   the statement
         * @param inputStream the input stream to set
         * @throws java.sql.SQLException if a sql exception occurs
         */
        void doWithStatement(Statement statement, InputStream inputStream) throws SQLException;
    }
}
