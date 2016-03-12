package com.opower.persistence.jpile.util;

import com.google.common.base.Throwables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Util class with JDBC routines
 *
 * @author ivan.german
 */
public final class JdbcTestUtil {

    private static final String JDBC_URL = "jdbc:mysql://localhost/jpile?useUnicode=true&characterEncoding=utf-8";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private JdbcTestUtil() {
    }

    /**
     * Open new database connection using defined credentials.
     * <p/>
     * <b>Important</b>: don't forget to close this connection.
     *
     * @return new {@link Connection}
     * @throws SQLException if connection failed to open
     */
    public static Connection openNewConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
    }
}
