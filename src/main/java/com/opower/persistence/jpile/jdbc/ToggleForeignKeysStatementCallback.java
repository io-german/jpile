package com.opower.persistence.jpile.jdbc;

import com.google.common.base.Preconditions;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * This implementation enables/disables foreign key constraints for current JDBC session.
 *
 * @author ivan.german
 */
public final class ToggleForeignKeysStatementCallback implements StatementCallback<Boolean> {

    public static final ToggleForeignKeysStatementCallback ENABLE_FOREIGN_KEYS =
            new ToggleForeignKeysStatementCallback(true);

    public static final ToggleForeignKeysStatementCallback DISABLE_FOREIGN_KEYS =
            new ToggleForeignKeysStatementCallback(false);

    private final boolean shouldEnable;

    private ToggleForeignKeysStatementCallback(boolean shouldEnable) {
        this.shouldEnable = shouldEnable;
    }

    @Override
    public Boolean doInStatement(Statement statement) throws SQLException {
        Preconditions.checkNotNull(statement, "statement can't be null");

        String query = String.format("SET FOREIGN_KEY_CHECKS = %d", this.shouldEnable ? 1 : 0);
        return statement.execute(query);
    }
}
