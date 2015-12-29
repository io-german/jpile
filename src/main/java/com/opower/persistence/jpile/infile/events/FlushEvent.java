package com.opower.persistence.jpile.infile.events;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This event indicates when infile buffer was flushed.
 *
 * @author phillip-michailov
 */
public class FlushEvent extends EventBase {

    private final Class<?> entityClass;
    private final String tableName;
    private final long timestamp;

    /**
     * Constructor
     *
     * @param source object that fired this event.
     * @param firePoint point in code path where this event occurred.
     * @param entityClass class annotated by {@link javax.persistence.Entity}.
     * @param tableName name of database table for which this event occurred.
     * @param timestamp when this event occurred in nanoseconds.
     */
    public FlushEvent(Object source, EventFirePoint firePoint, Class<?> entityClass, String tableName, long timestamp) {
        super(source, firePoint);
        this.entityClass = checkNotNull(entityClass, "Entity class cannot be null");
        this.tableName = checkNotNull(tableName, "Table name cannot be null");
        this.timestamp = timestamp;
    }

    /**
     * Constructs event by capturing current {@link System#nanoTime()} as {@link #timestamp}.
     */
    public FlushEvent(Object source, EventFirePoint firePoint, Class<?> entityClass, String tableName) {
        this(source, firePoint, entityClass, tableName, System.nanoTime());
    }

    /**
     * @return class annotated by {@link javax.persistence.Entity}.
     */
    public Class<?> getEntityClass() {
        return this.entityClass;
    }

    /**
     * @return when this event occurred in nanoseconds.
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * @return name of database table for which this event occurred.
     */
    public String getTableName() {
        return this.tableName;
    }
}
