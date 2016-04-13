package com.opower.persistence.jpile.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * <p>
 *  This implementation operates the {@link DataSource}. It creates a
 *  {@link ConnectionBasedStatementExecutor} using {@link java.sql.Connection}
 *  from {@link DataSource} and delegates all calls to it.
 * </p>
 * <p>
 *  Be aware that this implementation is not thread-safe.
 * </p>
 *
 * @author ivan.german
 */
public class DataSourceBasedStatementExecutor implements StatementExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceBasedStatementExecutor.class);

    private ConnectionBasedStatementExecutor connectionBasedStatementExecutor;
    private final DataSource dataSource;

    public DataSourceBasedStatementExecutor(DataSource dataSource) {
        Preconditions.checkNotNull(dataSource, "dataSource must not be null");

        this.dataSource = dataSource;
    }

    /**
     * <p>
     *  Processes {@link StatementCallback} in context of used connection.
     *  If this {@link StatementExecutor} don't own any open connection then new one
     *  is requested from the data source.
     * </p>
     * <p>
     *  Be aware that foreign key constraints are disabled for used connection.
     * </p>
     *
     * @throws RuntimeException if any problem occur during execution
     * @return result of {@link StatementCallback}
     */
    @Override
    public <T> T execute(StatementCallback<T> statementCallback) {
        try {
            if (this.connectionBasedStatementExecutor == null) {
                initNewConnection();
            }

            return this.connectionBasedStatementExecutor.execute(statementCallback);
        }
        /* Propagated exception */
        catch (Exception e) {
            try {
                shutdown();
            }
            catch (Exception shutdownException) {
                e.addSuppressed(shutdownException);
            }

            throw Throwables.propagate(e);
        }
    }

    /**
     * Re-enables foreign key constraints for owned connection and returns it
     * back to connection pool.
     *
     * @throws RuntimeException if any error occur during execution
     */
    @Override
    public void shutdown() {
        if (this.connectionBasedStatementExecutor != null) {
            try {
                this.connectionBasedStatementExecutor.shutdown();
            }
            /* Propagated exception should be logged and rethrown */
            catch (Exception e) {
                LOGGER.warn("Fail to re-enable foreign key constraints", e);
                throw Throwables.propagate(e);
            }
            finally {
                this.connectionBasedStatementExecutor.closeConnection();
                this.connectionBasedStatementExecutor = null;
            }
        }
    }

    /**
     * <p>
     *   Ensure that previous connection is closed and open new one.
     * </p>
     *
     * @throws RuntimeException if any problem occur
     */
    public void initNewConnection() {
        Connection connection = null;

        try {
            try {
                shutdown();
            }
            catch (RuntimeException ignoredException) {
                /*
                 * Exception from shutdown is already printed. Connection is returned to pool.
                 * So basically nothing restricts us from opening new connection.
                 */
            }

            connection = this.dataSource.getConnection();
            this.connectionBasedStatementExecutor = new ConnectionBasedStatementExecutor(connection);
        }
        /**
         * Exceptions propagated from {@link ConnectionBasedStatementExecutor}
         * and exception from {@link DataSource#getConnection()}
         */
        catch (Exception e) {
            LOGGER.error("Fail to init new connection", e);

            try {
                if (connection != null) {
                    connection.close();
                }
            }
            catch (SQLException closeException) {
                LOGGER.error("Fail to close connection", e);
                e.addSuppressed(closeException);
            }

            throw Throwables.propagate(e);
        }
    }
}
