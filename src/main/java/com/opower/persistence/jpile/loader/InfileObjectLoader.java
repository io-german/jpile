package com.opower.persistence.jpile.loader;

import com.google.common.base.Preconditions;
import com.opower.persistence.jpile.infile.InfileDataBuffer;
import com.opower.persistence.jpile.infile.InfileRow;
import com.opower.persistence.jpile.infile.InfileStatementCallback;
import com.opower.persistence.jpile.jdbc.ConnectionBasedStatementExecutor;
import com.opower.persistence.jpile.jdbc.StatementCallback;
import com.opower.persistence.jpile.jdbc.StatementExecutor;

import java.io.Flushable;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

/**
 * This class provides a convenient pattern for loading POJOs in batch to MySQL via tha 'LOAD DATA INFILE' protocol.
 * Given a configured data buffer, an infile SQL statement, and a connection, instances convert objects to rows
 * to infile rows in the data buffer. When said buffer is full, this class will automatically flush the contents of said
 * buffer to the database using the given SQL statement. Subclasses must only implement the translation of their
 * parameterized type to an infile row.
 * <p/>
 * Clients should be certain to call {@link #flush()} after they are finished adding objects to the loader via the
 * {@link #add(Object)} method. This will ensure that any objects that were not auto-flushed as a result of a full buffer
 * are pushed as well.
 * <p/>
 * Instances of this class are not safe for use by multiple threads.
 *
 * @param <E> entity to be converted to infile row
 * @author s-m
 * @see <a href="http://dev.mysql.com/doc/refman/5.1/en/load-data.html">LOAD DATA INFILE reference</a>
 * @since 1.0
 */
public abstract class InfileObjectLoader<E> implements Flushable {

    /**
     * Statement executor that will execute JDBC statements
     * @deprecated this field will eventually become private. Use corresponding accessor/mutator instead.
     */
    @Deprecated
    protected StatementExecutor statementExecutor;

    /**
     * Query to insert entities to database
     * @deprecated this field will eventually become private. Use corresponding accessor/mutator instead.
     */
    @Deprecated
    protected String loadInfileSql;

    /**
     * Buffer with entities prepared to insert to database
     * @deprecated this field will eventually become private. Use corresponding accessor/mutator instead.
     */
    @Deprecated
    protected InfileDataBuffer infileDataBuffer;

    /**
     * JDBC connection for entities insertion
     * @deprecated this field will be eventually removed. Use {@link #setStatementExecutor(StatementExecutor)}
     * with {@link ConnectionBasedStatementExecutor} instead.
     */
    @Deprecated
    protected Connection connection;

    // Lazy initialized. Normally, there will be none. If there are any there could be a ton, so we just
    // build a very large one if needed.
    private List<Exception> warnings;

    /**
     * For subclasses to extend correctly
     */
    protected InfileObjectLoader() {
    }

    /**
     * @return query to insert entities to database
     */
    protected String getLoadInfileSql() {
        return this.loadInfileSql;
    }

    protected void setLoadInfileSql(String loadInfileSql) {
        this.loadInfileSql = loadInfileSql;
    }

    /**
     * @return buffer with entities prepared to insert to database
     */
    protected InfileDataBuffer getInfileDataBuffer() {
        return this.infileDataBuffer;
    }

    protected void setInfileDataBuffer(InfileDataBuffer infileDataBuffer) {
        this.infileDataBuffer = infileDataBuffer;
    }

    /**
     * @return it is used to execute JDBC statements
     */
    protected StatementExecutor getStatementExecutor() {
        if (this.statementExecutor == null) {
            Preconditions.checkState(this.connection != null, "");
            this.statementExecutor = new ConnectionBasedStatementExecutor(this.connection);
        }
        return this.statementExecutor;
    }

    protected void setStatementExecutor(StatementExecutor statementExecutor) {
        if (this.statementExecutor != null) {
            this.statementExecutor.shutdown();
        }
        this.statementExecutor = statementExecutor;
    }

    /**
     * Add an entity to be written to the database as an infile row.
     *
     * @param entity to be added to infile, cannot be null
     */
    public void add(E entity) {
        Preconditions.checkNotNull(entity, "Entity to add cannot be null");

        this.convertToInfileRow(entity, this.infileDataBuffer.newRow());
        if (!this.infileDataBuffer.addRowToInfile()) {
            this.flush();
            if (!this.infileDataBuffer.addRowToInfile()) {
                // This should be impossible, as the buffer asserts that an empty infile can accept
                // any valid row.
                throw new IllegalStateException("Cannot add row to infile, even though infile has been flushed.");
            }
        }
    }

    /**
     * Gets the complete list of exceptions returned from all flush operations invoked upon this loader.
     *
     * @return errors that occurred during flushes
     */
    public List<Exception> getWarnings() {
        return this.warnings == null ? Collections.<Exception>emptyList() : this.warnings;
    }

    /**
     * Flushes the current contents of the infile buffer to the database, and then clears the buffer for writing.
     */
    @Override
    public void flush() {
        if (!this.infileDataBuffer.isEmptyInfileBuffer()) {
            StatementCallback<List<Exception>> statementCallback = new InfileStatementCallback(
                    this.loadInfileSql, this.infileDataBuffer.asInputStream()
            );
            this.warnings = getStatementExecutor().execute(statementCallback);
        }
        this.infileDataBuffer.clear();
    }

    /**
     * Adds data from a given entity to an infile row via said row's various <code>append</code> methods.
     *
     * @param entity    to convert
     * @param infileRow to which to append entity contents
     */
    public abstract void convertToInfileRow(E entity, InfileRow infileRow);
}
