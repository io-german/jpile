package com.opower.persistence.jpile.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * A helper callback method to process JDBC statements.
 *
 * @param <T> type of value returned form query
 * @author ivan.german
 */
public interface StatementCallback<T> {
    T doInStatement(Statement statement) throws SQLException;
}
