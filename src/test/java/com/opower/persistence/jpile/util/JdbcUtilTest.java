package com.opower.persistence.jpile.util;

import com.opower.persistence.jpile.loader.ConnectionHolder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.opower.persistence.jpile.util.JdbcUtil.StatementCallback;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link JdbcUtil} for fault tolerance.
 *
 * @author ivan.german
 */
public class JdbcUtilTest {

    private static final Boolean SUCCESS_RESULT = Boolean.TRUE;

    /*
     * Don't forget to initialize this mock with some behavior before using!
     */
    @Mock
    private StatementCallback<Boolean> mockStatementCallback;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Given {@link StatementCallback} that results successfully
     * When pass it to {@link JdbcUtil#execute(ConnectionHolder, StatementCallback, org.springframework.retry.RetryPolicy)}
     * Then result should be successfully returned
     */
    @Test
    public void testSuccess() throws SQLException {
        setupSuccessCallback();
        Object actualResult = JdbcUtil.execute(mockConnectionHolder(), mockStatementCallback, new NeverRetryPolicy());
        assertEquals(SUCCESS_RESULT, actualResult);
    }

    /**
     * Given {@link StatementCallback} that results unsuccessfully
     * When pass it to {@link JdbcUtil#execute(ConnectionHolder, StatementCallback, org.springframework.retry.RetryPolicy)}
     *              method with {@link NeverRetryPolicy}
     * Then exception must be thrown from this call
     */
    @Test(expected = RuntimeException.class)
    public void testFailure() throws SQLException {
        setupBlinkingCallback();
        JdbcUtil.execute(mockConnectionHolder(), mockStatementCallback, new NeverRetryPolicy());
    }

    /**
     * Given {@link StatementCallback} that results unsuccessfully
     * When pass it to {@link JdbcUtil#execute(ConnectionHolder, StatementCallback, org.springframework.retry.RetryPolicy)}
     *              method with {@link AlwaysRetryPolicy}
     * Then result should be successfully returned
     *      IF any of subsequent calls is successful
     */
    @Test
    public void testSuccessAfterRetry() throws SQLException {
        setupBlinkingCallback();
        Object actualResult = JdbcUtil.execute(mockConnectionHolder(), mockStatementCallback, new AlwaysRetryPolicy());
        assertEquals(SUCCESS_RESULT, actualResult);
    }

    private ConnectionHolder mockConnectionHolder() throws SQLException {
        DataSource mockDataSource = mock(DataSource.class);

        Connection mockConnection1 = mock(Connection.class);
        when(mockConnection1.createStatement()).thenReturn(mock(Statement.class));

        Connection mockConnection2 = mock(Connection.class);
        when(mockConnection2.createStatement()).thenReturn(mock(Statement.class));

        when(mockDataSource.getConnection()).thenReturn(mockConnection1, mockConnection2);

        return new ConnectionHolder(mockDataSource);
    }

    private void setupSuccessCallback() throws SQLException {
        when(mockStatementCallback.doInStatement(any(Statement.class))).thenReturn(SUCCESS_RESULT);
    }


    private void setupBlinkingCallback() throws SQLException {
        when(mockStatementCallback.doInStatement(any(Statement.class)))
                .thenThrow(new SQLException()).thenReturn(SUCCESS_RESULT);
    }
}
