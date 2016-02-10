package com.opower.persistence.jpile.loader;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.opower.persistence.jpile.util.JdbcUtil;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.retry.policy.NeverRetryPolicy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A holder for database connections. It is used to perform continuous work with
 * single database connection. If for some reasons connection fails it just takes
 * another one from underlying {@link DataSource}
 *
 * @author ivan.german
 */
public class ConnectionHolder {

    private Connection currentConnection;
    private final DataSource dataSource;

    /**
     * Initializes this {@link ConnectionHolder} with single connection.
     *
     * @param currentConnection {@link Connection} to set.
     */
    public ConnectionHolder(Connection currentConnection) {
        Preconditions.checkNotNull(currentConnection, "current connection must be defined");
        this.dataSource = new SingleConnectionDataSource(currentConnection, true);
    }

    /**
     * Initializes this ConnectionHolder with {@link DataSource}.
     *
     * Each time you {@link #resetCurrentConnection()} and then {@link #getConnection()}
     * you'll get new one according to DataSource implementation.
     *
     * @param dataSource {@link DataSource} to set
     */
    public ConnectionHolder(DataSource dataSource) {
        Preconditions.checkNotNull(dataSource, "dataSource must be defined");
        this.dataSource = dataSource;
    }

    /**
     * Returns current {@link Connection} if it is defined. If it's not then take one
     * from the underlying {@link DataSource}. Foreign key constraints are disabled for
     * {@link Connection}. Don't forget to call {@link #resetCurrentConnection()} to
     * re-enable foreign key constraint for current database session.
     *
     * @return current JDBC {@link Connection}
     */
    public synchronized Connection getConnection() {
        try {
            if (this.currentConnection == null || this.currentConnection.isClosed()) {
                enableNewConnection();
            }
            return this.currentConnection;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Re-enables foreign key constraints for current {@link Connection} and undefines it.
     * Next call to {@link #getConnection()} will take another connection from
     * data source.
     */
    public synchronized void resetCurrentConnection() {
        try {
            disableCurrentConnection();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private void enableNewConnection() throws SQLException {
        this.currentConnection = this.dataSource.getConnection();
    }

    private void disableCurrentConnection() throws SQLException {
        try {
            /* If currentConnection == null then it's either not initialized or already reset */
            if (this.currentConnection != null) {
                JdbcUtil.execute(this, ENABLE_FOREIGN_KEYS, new NeverRetryPolicy());
                this.currentConnection.close();
            }
        }
        finally {
            this.currentConnection = null;
        }
    }

    private static final JdbcUtil.StatementCallback<Boolean> DISABLE_FOREIGN_KEYS = new JdbcUtil.StatementCallback<Boolean>() {
        @Override
        public Boolean doInStatement(Statement statement) throws SQLException {
            return statement.execute("SET FOREIGN_KEY_CHECKS = 0");
        }
    };

    private static final JdbcUtil.StatementCallback<Boolean> ENABLE_FOREIGN_KEYS = new JdbcUtil.StatementCallback<Boolean>() {
        @Override
        public Boolean doInStatement(Statement statement) throws SQLException {
            return statement.execute("SET FOREIGN_KEY_CHECKS = 1");
        }
    };

}
