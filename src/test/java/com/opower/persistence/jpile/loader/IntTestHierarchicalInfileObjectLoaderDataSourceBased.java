package com.opower.persistence.jpile.loader;

import com.google.common.base.Throwables;
import com.opower.persistence.jpile.jdbc.DataSourceBasedStatementExecutor;
import com.opower.persistence.jpile.jdbc.StatementExecutor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests object loader for correctness using {@link com.opower.persistence.jpile.jdbc.DataSourceBasedStatementExecutor}.
 *
 * @author ivan.german
 */
public class IntTestHierarchicalInfileObjectLoaderDataSourceBased
        extends IntTestHierarchicalInfileObjectLoaderConnectionBased {

    @Override
    public StatementExecutor getStatementExecutor(Connection connection) {
        try {
            DataSource dataSource = mock(DataSource.class);
            when(dataSource.getConnection()).thenReturn(connection);

            return new DataSourceBasedStatementExecutor(dataSource);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
