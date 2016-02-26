package com.opower.persistence.jpile.jdbc;

/**
 * Interface that provides basic ability to process JDBC {@link java.sql.Statement}
 * using {@link StatementCallback}.
 *
 * @author ivan.german
 */
public interface StatementExecutor {

    /**
     * Processes {@link StatementCallback}
     *
     * @param statementCallback callback to execute
     * @param <T> type of value to return
     * @return value from callback
     */
    <T> T execute(StatementCallback<T> statementCallback);

    /**
     * Perform some on-close action (e.g. close database connection).
     */
    void shutdown();
}
