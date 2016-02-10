package com.opower.persistence.jpile.loader;

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ConnectionHolder} for correctness.
 *
 * @author ivan.german
 */
public class ConnectionHolderTest {

    /**
     * Given {@link ConnectionHolder} built with {@link DataSource}
     * When call {@code getConnection()} method on it
     * Then {@link Connection} taken from the {@link DataSource} should be returned
     */
    @Test
    public void testGetConnectionFromDataSource() throws SQLException {
        Statement mockStatement = mock(Statement.class);
        Connection mockConnection = mock(Connection.class);
        DataSource mockDataSource = mock(DataSource.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.execute(anyString())).thenReturn(true);

        ConnectionHolder connectionHolder = new ConnectionHolder(mockDataSource);

        assertEquals(mockConnection, connectionHolder.getConnection());
    }

    /**
     * Given {@link ConnectionHolder} built with {@link DataSource}
     * When call {@code getConnection()}, {@code resetCurrentConnection()} and {@code getConnection()} methods on it
     * Then different connections should be returned
     */
    @Test
    public void testGetConnectionFromDataSourceAfterReset() throws SQLException {
        Statement mockStatement = mock(Statement.class);
        Connection mockConnection1 = mock(Connection.class);
        Connection mockConnection2 = mock(Connection.class);
        DataSource mockDataSource = mock(DataSource.class);

        assertNotEquals(mockConnection1, mockConnection2);

        when(mockDataSource.getConnection()).thenReturn(mockConnection1, mockConnection2);
        when(mockConnection1.createStatement()).thenReturn(mockStatement);
        when(mockConnection2.createStatement()).thenReturn(mockStatement);
        when(mockStatement.execute(anyString())).thenReturn(true);

        ConnectionHolder connectionHolder = new ConnectionHolder(mockDataSource);

        connectionHolder.getConnection();
        connectionHolder.resetCurrentConnection();
        assertEquals(mockConnection2, connectionHolder.getConnection());
    }

    /**
     * Given {@link ConnectionHolder} built with {@link DataSource}
     * When call {@code getConnection()} method on it and data source throws an {@link SQLException}
     * Then it should be wrapped with {@link RuntimeException}
     */
    @Test(expected = RuntimeException.class)
    public void testGetConnectionFromDatasourceFailure() throws SQLException {
        DataSource mockDataSource = mock(DataSource.class);

        when(mockDataSource.getConnection()).thenThrow(new SQLException());

        new ConnectionHolder(mockDataSource).getConnection();
    }

}
