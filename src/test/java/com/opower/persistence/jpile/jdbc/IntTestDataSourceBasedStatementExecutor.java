package com.opower.persistence.jpile.jdbc;

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.opower.persistence.jpile.util.JdbcTestUtil.openNewConnection;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test to check that {@link DataSourceBasedStatementExecutor} works correctly.
 *
 * @author ivan.german
 */
public class IntTestDataSourceBasedStatementExecutor {

    /**
     * Create {@link DataSourceBasedStatementExecutor}, execute query on it and shut it down.
     */
    @Test
    public void testExecute() throws SQLException {
        final String query = "SELECT 1";

        Connection connection = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);

        DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);

        boolean result = executor.execute(new StatementCallback<Boolean>() {
            @Override
            public Boolean doInStatement(Statement statement) throws SQLException {
                return statement.execute(query);
            }
        });

        assertTrue(result);

        executor.shutdown();
    }

    /**
     * Create {@link DataSourceBasedStatementExecutor}, shut it down and init new connection.
     */
    @Test
    public void testShutdownAndInitialize() throws SQLException {
        Connection connection1 = openNewConnection();
        Connection connection2 = openNewConnection();

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection1, connection2);

        DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
        executor.shutdown();
        executor.initNewConnection();

        assertTrue(connection1.isClosed());
        assertFalse(connection2.isClosed());

        executor.shutdown();

        assertTrue(connection2.isClosed());
    }
}
