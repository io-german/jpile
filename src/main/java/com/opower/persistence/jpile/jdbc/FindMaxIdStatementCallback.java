package com.opower.persistence.jpile.jdbc;

import com.google.common.base.Preconditions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This implementation returns the max ID of the table. If ID column name
 * is not defined 0 is returned.
 *
 * @author ivan.german
 */
public class FindMaxIdStatementCallback implements StatementCallback<Long> {

    private static final String QUERY_TEMPLATE = "select max(%s) from %s";

    private final String idColumnName;
    private final String tableName;

    public FindMaxIdStatementCallback(String idColumnName, String tableName) {
        Preconditions.checkNotNull(tableName, "tableName is required");

        this.idColumnName = idColumnName;
        this.tableName = tableName;
    }

    @Override
    public Long doInStatement(Statement statement) throws SQLException {
        Preconditions.checkNotNull(statement, "statement can't be null");

        if (idColumnName == null) {
            return 0L;
        }

        String query = String.format(QUERY_TEMPLATE, this.idColumnName, this.tableName);

        try (ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.first()) {
                return resultSet.getLong(1);
            }
            throw new SQLException("Could not find max id for table [%s]", tableName);
        }
    }

}
