package com.opower.persistence.jpile;

import com.google.common.collect.ImmutableList;
import com.opower.persistence.jpile.jdbc.ConnectionBasedStatementExecutor;
import com.opower.persistence.jpile.jdbc.StatementExecutor;
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
import java.util.List;

import static com.opower.persistence.jpile.util.JdbcTestUtil.openNewConnection;

/**
 * Abstract test case for all int tests. Loads MySQL drivers and creates a new MySQL {@link Connection}
 *
 * @author amir.raminfar
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public abstract class AbstractIntTestForJPile {
    private static final List<String> TABLES =
            ImmutableList.of("customer", "product", "contact", "contact_phone", "binary_data", "supplier");

    /**
     * Free connection that can be used to execute standalone queries
     */
    protected Connection connection;

    /**
     * Infile object loader.
     */
    protected HierarchicalInfileObjectLoader hierarchicalInfileObjectLoader = new HierarchicalInfileObjectLoader();

    /**
     * Spring's JDBC template wrapped around {@link #connection}
     */
    protected JdbcTemplate jdbcTemplate;

    @BeforeClass
    public static void createTables() throws Exception {
        Connection connection = openNewConnection();
        JdbcTestUtils.executeSqlScript(
                new JdbcTemplate(new SingleConnectionDataSource(connection, true)),
                new InputStreamResource(AbstractIntTestForJPile.class.getResourceAsStream("/jpile.sql")),
                false
        );
        connection.close();
    }

    @Before
    public void setUp() throws Exception {
        this.connection = openNewConnection();

        Connection statementExecutorConnection = openNewConnection();
        this.hierarchicalInfileObjectLoader.setStatementExecutor(getStatementExecutor(statementExecutorConnection));

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
        Connection connection = openNewConnection();
        JdbcTemplate template = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
        for (String table : TABLES) {
            template.update("drop table " + table);
        }
        connection.close();
    }

    /**
     * Prepares custom {@link StatementExecutor} to {@link #hierarchicalInfileObjectLoader}.
     * Default implementation uses {@link ConnectionBasedStatementExecutor} which can be changed by
     * overriding this method.
     *
     * @param connection connection to the database
     * @return {@link StatementExecutor} instance
     */
    public StatementExecutor getStatementExecutor(Connection connection) {
        return new ConnectionBasedStatementExecutor(connection);
    }
}
