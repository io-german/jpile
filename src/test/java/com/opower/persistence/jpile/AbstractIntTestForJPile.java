package com.opower.persistence.jpile;

import com.google.common.base.Throwables;
import com.opower.persistence.jpile.loader.HierarchicalInfileObjectLoader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.core.io.InputStreamResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;

/**
 * Abstract test case for all int tests. Loads MySQL drivers and creates a new MySQL {@link Connection}
 *
 * @author amir.raminfar
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public abstract class AbstractIntTestForJPile {
    private static final String JDBC_URL = "jdbc:mysql://localhost/jpile?useUnicode=true&characterEncoding=utf-8";
    private static final List<String> TABLES = of("customer", "product", "contact", "contact_phone", "binary_data", "supplier");
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

    protected Connection connection;
    protected HierarchicalInfileObjectLoader hierarchicalInfileObjectLoader = new HierarchicalInfileObjectLoader();
    protected JdbcTemplate jdbcTemplate;

    @BeforeClass
    public static void createTables() throws Exception {
        Connection connection = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
        JdbcTestUtils.executeSqlScript(
                new JdbcTemplate(new SingleConnectionDataSource(connection, true)),
                new InputStreamResource(AbstractIntTestForJPile.class.getResourceAsStream("/jpile.sql")),
                false
        );
        connection.close();
    }

    @Before
    public void setUp() throws Exception {
        this.connection = DriverManager.getConnection(JDBC_URL, DB_USER, "");
        this.hierarchicalInfileObjectLoader.setConnection(this.connection);
        this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(this.connection, true));
    }

    @After
    public void tearDown() throws Exception {
        this.hierarchicalInfileObjectLoader.close();
        for (String table : TABLES) {
            this.jdbcTemplate.update("truncate " + table);
        }
        this.connection.close();
    }

    @AfterClass
    public static void dropTables() throws Exception {
        Connection connection = DriverManager.getConnection(JDBC_URL, DB_USER, "");
        JdbcTemplate template = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
        for (String table : TABLES) {
            template.update("drop table " + table);
        }
        connection.close();
    }
}
