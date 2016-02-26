package com.opower.persistence.jpile.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.opower.persistence.jpile.jdbc.ConnectionBasedStatementExecutor;

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
     * @param connection        the connection to use
     * @param statementCallback the callback
     * @param <E>               the return type
     * @return the return value from callback
     *
     * @deprecated use {@link com.opower.persistence.jpile.jdbc.StatementExecutor}.
     */
    @Deprecated
    public static <E> E execute(Connection connection, final StatementCallback<E> statementCallback) {
        return new ConnectionBasedStatementExecutor(connection)
                .execute(transformCallback(statementCallback));
    }

    private static <T> com.opower.persistence.jpile.jdbc.StatementCallback<T>
            transformCallback(final StatementCallback<T> callback) {

        return new com.opower.persistence.jpile.jdbc.StatementCallback<T>() {
            @Override
            public T doInStatement(Statement statement) throws SQLException {
                return callback.doInStatement(statement);
            }
        };
    }

    /**
     * A helper callback method for this util class
     *
     * @deprecated use {@link com.opower.persistence.jpile.jdbc.StatementCallback} instead
     */
    @Deprecated
    public interface StatementCallback<E> {
        /**
         * @param statement the statement to use
         * @return return value for this callback
         * @throws SQLException if a sql error happens
         */
        E doInStatement(Statement statement) throws SQLException;
    }
}
