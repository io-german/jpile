package com.opower.persistence.jpile.util;

import com.google.common.base.Throwables;
import com.opower.persistence.jpile.loader.ConnectionHolder;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.support.RetryTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A jdbc helper util class. This class was created so that Spring JDBC is no longer a dependency.
 *
 * @author amir.raminfar
 */
public final class JdbcUtil {
    private JdbcUtil() {
    }

    /**
     * Creates a new statement from connection and closes the statement properly after
     *
     * TODO: make this method private
     *
     * @param connection        the connection to use
     * @param statementCallback the callback
     * @param <E>               the return type
     * @return the return value from callback
     *
     * @deprecated this method will become private eventually.
     * Use {@link #execute(ConnectionHolder, StatementCallback, RetryPolicy)} instead.
     *
     */
    @Deprecated
    public static <E> E execute(Connection connection, StatementCallback<E> statementCallback) {
        try (Statement statement = connection.createStatement()) {
            return statementCallback.doInStatement(statement);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <E> E execute(ConnectionHolder connectionHolder,
                                StatementCallback<E> statementCallback,
                                RetryPolicy retryPolicy) {

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setListeners(new RetryListener[] {new TryAnotherConnectionListener(connectionHolder)});
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate.execute(new RetriableStatement<>(connectionHolder, statementCallback));
    }

    private static class RetriableStatement<E> implements RetryCallback<E, RuntimeException> {
        private final ConnectionHolder connectionHolder;
        private final StatementCallback<E> statementCallback;

        public RetriableStatement(ConnectionHolder connectionHolder, StatementCallback<E> statementCallback) {
            this.connectionHolder = connectionHolder;
            this.statementCallback = statementCallback;
        }

        @Override
        public E doWithRetry(RetryContext context) throws RuntimeException {
            Connection connection = connectionHolder.getConnection();
            return execute(connection, statementCallback);
        }
    }

    private static class TryAnotherConnectionListener extends RetryListenerSupport {
        private final ConnectionHolder connectionHolder;

        public TryAnotherConnectionListener(ConnectionHolder connectionHolder) {
            this.connectionHolder = connectionHolder;
        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            this.connectionHolder.resetCurrentConnection();
        }
    }

    /**
     * A helper callback method for this util class
     */
    public interface StatementCallback<E> {
        /**
         * @param statement the statement to use
         * @return return value for this callback
         * @throws SQLException if a sql error happens
         */
        E doInStatement(Statement statement) throws SQLException;
    }
}
