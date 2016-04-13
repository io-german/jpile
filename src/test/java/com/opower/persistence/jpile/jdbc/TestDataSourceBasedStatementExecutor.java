package com.opower.persistence.jpile.jdbc;

import com.google.common.base.Throwables;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test to check that {@link DataSourceBasedStatementExecutor} works correctly.
 *
 * @author ivan.german
 */
public class TestDataSourceBasedStatementExecutor {

    private final Exception failureException = new SQLException("Expected failure exception");

    /**
     * Create {@link DataSourceBasedStatementExecutor} and execute statement on it.
     * Expected actions:
     * <ol>
     *     <li>Init new connection</li>
     *     <li>Execute statement</li>
     *     <li>Connection should <b>not</b> be closed at the end</li>
     * </ol>
     *
     * @throws SQLException
     */
    @Test
    public void testExecute() throws SQLException {
        Statement disableFkStatement = mockStatement(true);
        Statement statement = mockStatement(true);
        Connection connection = mockConnection(disableFkStatement, statement);

        DataSource dataSource = mockDataSource(connection);

        DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
        executor.execute(new MockStatementCallback());

        verify(disableFkStatement).close();
        verify(statement).close();

        /* Last statement was successful so connection will be reused */
        verify(connection, never()).close();
    }

    /**
     * Create {@link DataSourceBasedStatementExecutor} and execute two statements on it.
     * Expected actions:
     * <ol>
     *     <li>Init new connection</li>
     *     <li>Execute statement1</li>
     *     <li>Execute statement2 using the same connection</li>
     *     <li>Connection should <b>not</b> be closed at the end</li>
     * </ol>
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteSeveralStatements() throws SQLException {
        Statement disableFkStatement = mockStatement(true);
        Statement statement1 = mockStatement(true);
        Statement statement2 = mockStatement(true);

        Connection connection = mockConnection(disableFkStatement, statement1, statement2);

        DataSource dataSource = mockDataSource(connection);

        DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
        executor.execute(new MockStatementCallback());
        executor.execute(new MockStatementCallback());

        verify(disableFkStatement).close();
        verify(statement1).close();
        verify(statement2).close();

        /* Last statement was successful so connection will be reused */
        verify(connection, never()).close();
    }

    /**
     * Create {@link DataSourceBasedStatementExecutor}, execute statement on it, shut it down and execute another statement.
     * Expected actions:
     * <ol>
     *     <li>Init new connection</li>
     *     <li>Execute statement</li>
     *     <li>Close connection</li>
     *     <li>Init new connection</li>
     *     <li>Execute second statement</li>
     *     <li>Connection should <b>not</b> be closed at the end</li>
     * </ol>
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteThenShutdownThenExecute() throws SQLException {
        Statement disableFkStatement1 = mockStatement(true);
        Statement statement1 = mockStatement(true);
        Statement enableFkStatement1 = mockStatement(true);
        Connection connection1 = mockConnection(disableFkStatement1, statement1, enableFkStatement1);

        Statement disableFkStatement2 = mockStatement(true);
        Statement statement2 = mockStatement(true);
        Connection connection2 = mockConnection(disableFkStatement2, statement2);

        DataSource dataSource = mockDataSource(connection1, connection2);

        DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);

        executor.execute(new MockStatementCallback());
        executor.shutdown();

        verify(disableFkStatement1).close();
        verify(statement1).close();
        verify(enableFkStatement1).close();
        verify(connection1).close();

        executor.execute(new MockStatementCallback());

        verify(disableFkStatement2).close();
        verify(statement2).close();

        /* Last statement was successful so connection will be reused */
        verify(connection2, never()).close();
    }

    /**
     * Check that connection is returned to pool if it fails to initialize (disable foreign key constraints).
     *
     * @throws Throwable
     */
    @Test
    public void testDisableForeignKeysFailure() throws Throwable {
        Statement disableFkStatement = mockStatement(false);
        Connection connection = mockConnection(disableFkStatement);

        DataSource dataSource = mockDataSource(connection);

        try {
            DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
            executor.execute(new MockStatementCallback());
        }
        catch (RuntimeException e) {
            assertEquals(this.failureException, Throwables.getRootCause(e));
        }
        finally {
            verify(disableFkStatement).close();
            verify(connection).close();
        }
    }

    /**
     * Check that connection is closed if it fails to execute statement.
     *
     * @throws Throwable
     */
    @Test
    public void testExecutionFailure() throws Throwable {
        Statement disableFkStatement = mockStatement(true);
        Statement statement = mockStatement(false);
        Statement enableFkStatement = mockStatement(true);

        Connection connection = mockConnection(disableFkStatement, statement, enableFkStatement);

        DataSource dataSource = mockDataSource(connection);

        try {
            DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
            executor.execute(new MockStatementCallback());
        }
        catch (RuntimeException e) {
            assertEquals(this.failureException, Throwables.getRootCause(e));
        }
        finally {
            verify(disableFkStatement).close();
            verify(statement).close();
            verify(enableFkStatement).close();
            verify(connection).close();
        }
    }

    /**
     * Check that connection is closed if it fails to execute statement and re-enable foreign key constraints.
     * Also check that exception from {@link DataSourceBasedStatementExecutor#execute(StatementCallback)}
     * is passed to the caller instead of exception from {@link DataSourceBasedStatementExecutor#shutdown()}
     *
     * @throws Throwable
     */
    @Test
    public void testReEnableForeignKeysFailure() throws Throwable {
        Statement disableFkStatement = mockStatement(true);
        Statement statement = mockStatement(false);
        Statement enableFkStatement = mockStatement(false, new SQLException("Exception during re-enabling of foreign keys"));

        Connection connection = mockConnection(disableFkStatement, statement, enableFkStatement);

        DataSource dataSource = mockDataSource(connection);

        try {
            DataSourceBasedStatementExecutor executor = new DataSourceBasedStatementExecutor(dataSource);
            executor.execute(new MockStatementCallback());
        }
        catch (RuntimeException e) {
            assertEquals(this.failureException, Throwables.getRootCause(e));
        }
        finally {
            verify(disableFkStatement).close();
            verify(statement).close();
            verify(enableFkStatement).close();
            verify(connection).close();
        }
    }

    private Statement mockStatement(boolean successful) throws SQLException {
        return mockStatement(successful, this.failureException);
    }

    private Statement mockStatement(boolean successful, Exception thrownException) throws SQLException {
        Statement result = mock(Statement.class);

        if (successful) {
            when(result.execute(anyString())).thenReturn(true);
        }
        else {
            when(result.execute(anyString())).thenThrow(thrownException);
        }

        return result;
    }

    private Connection mockConnection(Statement... statements) throws SQLException {
        Connection result = mock(Connection.class);

        OngoingStubbing<Statement> expectedCall = when(result.createStatement());
        for (Statement statement : statements) {
            expectedCall = expectedCall.thenReturn(statement);
        }

        return result;
    }

    private DataSource mockDataSource(Connection... connections) throws SQLException {
        DataSource result = mock(DataSource.class);

        OngoingStubbing<Connection> expectedCall = when(result.getConnection());
        for (Connection connection : connections) {
            expectedCall = expectedCall.thenReturn(connection);
        }

        return result;
    }

    private class MockStatementCallback implements StatementCallback<Boolean> {
        @Override
        public Boolean doInStatement(Statement statement) throws SQLException {
            return statement.execute("");
        }
    }
}
