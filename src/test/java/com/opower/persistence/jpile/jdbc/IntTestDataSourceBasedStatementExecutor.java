package com.opower.persistence.jpile.jdbc;

import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.opower.persistence.jpile.util.JdbcTestUtil.openNewConnection;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test to check that {@link DataSourceBasedStatementExecutor} works correctly.
 *
 * @author ivan.german
 */
public class IntTestDataSourceBasedStatementExecutor {

    private DataSourceBasedStatementExecutor executor;

    @After
    public void tearDown() {
        if (this.executor != null) {
            this.executor.shutdown();
        }
    }

    /**
     * Execute query using {@link DataSourceBasedStatementExecutor}.
     */
    @Test
    public void testExecute() throws SQLException {
        String query = "SELECT 1";

        Connection connection = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);

        this.executor = new DataSourceBasedStatementExecutor(dataSource);

        boolean result = this.executor.execute(statementCallbackForQuery(query));

        assertTrue(result);
    }

    /**
     * Execute query and close {@link DataSourceBasedStatementExecutor}.
     * Check that connection is closed.
     */
    @Test
    public void testShutdownExecutor() throws SQLException {
        String query = "SELECT 1";

        Connection connection = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);

        this.executor = new DataSourceBasedStatementExecutor(dataSource);

        this.executor.execute(statementCallbackForQuery(query));
        this.executor.shutdown();

        assertTrue(connection.isClosed());
    }

    /**
     * Shut {@link DataSourceBasedStatementExecutor} down and init new connection.
     * Check that first connection is closed and second one is opened.
     */
    @Test
    public void testShutdownAndInitialize() throws SQLException {
        Connection connection1 = openNewConnection();
        Connection connection2 = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection1, connection2);

        this.executor = new DataSourceBasedStatementExecutor(dataSource);
        this.executor.initNewConnection();
        this.executor.shutdown();
        this.executor.initNewConnection();

        assertTrue(connection1.isClosed());
        assertFalse(connection2.isClosed());
    }

    /**
     * Execute query that fails and check that connection is closed automatically.
     */
    @Test
    public void testExecuteQueryThatFails() throws Throwable {
        Connection connection = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);

        this.executor = new DataSourceBasedStatementExecutor(dataSource);

        try {
            String query = "invalid query";
            this.executor.execute(statementCallbackForQuery(query));
            fail("Exception must be thrown in the previous line");
        }
        catch (Exception e) {
            /* Ignore this exception. We know that syntax is invalid. */
        }
        finally {
            assertTrue(connection.isClosed());
        }
    }

    private StatementCallback<Boolean> statementCallbackForQuery(final String query) {
        return new StatementCallback<Boolean>() {
            @Override
            public Boolean doInStatement(Statement statement) throws SQLException {
                return statement.execute(query);
            }
        };
    }
}
