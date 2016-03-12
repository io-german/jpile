package com.opower.persistence.jpile.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * This implementation operates the {@link DataSource}. It creates a
 * {@link ConnectionBasedStatementExecutor} using {@link java.sql.Connection}
 * from {@link DataSource} and delegates all calls to it.
 * <p/>
 * This implementation has no mechanism to process failures from underlying
 * {@link ConnectionBasedStatementExecutor} and retry execution with another
 * {@link java.sql.Connection}. If you need such behavior then create a subclass.
 * <p/>
 * Be aware that this implementation is not thread-safe.
 *
 * @author ivan.german
 */
public class DataSourceBasedStatementExecutor implements StatementExecutor {

    private ConnectionBasedStatementExecutor connectionBasedStatementExecutor;
    private final DataSource dataSource;

    public DataSourceBasedStatementExecutor(DataSource dataSource) throws SQLException {
        Preconditions.checkNotNull(dataSource, "dataSource must not be null");

        this.dataSource = dataSource;
        initNewConnection();
    }

    /**
     * @inheritDoc
     */
    @Override
    public <T> T execute(StatementCallback<T> statementCallback) {
        Preconditions.checkNotNull(
                this.connectionBasedStatementExecutor, "underlying connectionBasedStatementExecutor is null");

        return this.connectionBasedStatementExecutor.execute(statementCallback);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void shutdown() {
        if (this.connectionBasedStatementExecutor != null) {
            this.connectionBasedStatementExecutor.shutdown();
            this.connectionBasedStatementExecutor.closeConnection();
            this.connectionBasedStatementExecutor = null;
        }
    }

    /**
     * Ensure that previous connection is closed and open new one.
     */
    public void initNewConnection() {
        try {
            shutdown();
            this.connectionBasedStatementExecutor = new ConnectionBasedStatementExecutor(dataSource.getConnection());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
