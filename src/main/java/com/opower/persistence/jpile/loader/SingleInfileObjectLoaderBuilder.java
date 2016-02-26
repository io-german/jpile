package com.opower.persistence.jpile.loader;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.opower.persistence.jpile.infile.InfileDataBuffer;
import com.opower.persistence.jpile.jdbc.ConnectionBasedStatementExecutor;
import com.opower.persistence.jpile.jdbc.FindMaxIdStatementCallback;
import com.opower.persistence.jpile.jdbc.StatementExecutor;
import com.opower.persistence.jpile.reflection.PersistenceAnnotationInspector;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.SecondaryTable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The builder for creating a SingleInfileObjectLoader. This class does the building and parsing of the annotations.
 *
 * @param <E> the type of object which this class will support
 * @author amir.raminfar
 * @see SingleInfileObjectLoader
 */
public class SingleInfileObjectLoaderBuilder<E> {

    private final EventBus eventBus;
    private final Class<? extends E> aClass;
    private StatementExecutor statementExecutor;
    private InfileDataBuffer infileDataBuffer;
    private PersistenceAnnotationInspector annotationInspector;
    private String tableName;
    private boolean defaultTableName = false;
    private boolean allowNull = false;
    private boolean embedded = false;
    private boolean useReplace = false;
    private SecondaryTable secondaryTable;

    public SingleInfileObjectLoaderBuilder(Class<? extends E> aClass, EventBus eventBus) {
        this.aClass = checkNotNull(aClass, "Class cannot be null");
        this.eventBus = checkNotNull(eventBus, "Event bus cannot be null");
    }

    public SingleInfileObjectLoaderBuilder<E> withBuffer(InfileDataBuffer infileDataBuffer) {
        this.infileDataBuffer = infileDataBuffer;
        return this;
    }

    /**
     * @deprecated use {@link #withStatementExecutor(StatementExecutor)} instead
     */
    @Deprecated
    public SingleInfileObjectLoaderBuilder<E> withJdbcConnection(Connection connection) {
        Preconditions.checkNotNull(connection, "connection cannot be null");
        this.statementExecutor = new ConnectionBasedStatementExecutor(connection);
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> withStatementExecutor(StatementExecutor statementExecutor) {
        Preconditions.checkNotNull(statementExecutor, "statement executor cannot be null");
        this.statementExecutor = statementExecutor;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> usingAnnotationInspector(PersistenceAnnotationInspector annotationInspector) {
        this.annotationInspector = annotationInspector;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> withTableName(String tableName) {
        this.defaultTableName = false;
        this.tableName = tableName;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> withDefaultTableName() {
        this.defaultTableName = true;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> allowNull() {
        this.allowNull = true;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> doNotAllowNull() {
        this.allowNull = false;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> useReplace(boolean useReplace) {
        this.useReplace = useReplace;
        return this;
    }

    public SingleInfileObjectLoaderBuilder<E> usingSecondaryTable(SecondaryTable secondaryTable) {
        this.secondaryTable = secondaryTable;
        return this;
    }

    private SingleInfileObjectLoaderBuilder<E> isEmbedded() {
        this.embedded = true;
        return this;
    }

    /**
     * Builds the object loader by looking at all the annotations and returns a new object loader.
     *
     * @return a new instance of object loader
     */
    public SingleInfileObjectLoader<E> build() {
        checkNotNull(this.statementExecutor, "statementExecutor cannot be null");
        checkNotNull(this.annotationInspector, "persistenceAnnotationInspector cannot be null");
        checkNotNull(this.infileDataBuffer, "infileDataBuffer cannot be null");

        SingleInfileObjectLoader<E> objectLoader = new SingleInfileObjectLoader<>(this.aClass, this.eventBus);
        objectLoader.statementExecutor = this.statementExecutor;
        objectLoader.infileDataBuffer = this.infileDataBuffer;
        objectLoader.persistenceAnnotationInspector = this.annotationInspector;
        objectLoader.allowNull = this.allowNull;
        objectLoader.embedChild = this.embedded;
        if (this.defaultTableName) {
            if (this.secondaryTable == null) {
                this.tableName = this.annotationInspector.tableName(this.aClass);
            }
            else {
                this.tableName = this.secondaryTable.name();
            }
        }
        objectLoader.tableName = checkNotNull(this.tableName, "tableName cannot be null");
        this.findAnnotations(objectLoader);
        if (!this.embedded) {
            String idColumnName = findPrimaryIdColumnName(objectLoader);
            objectLoader.setAutoGeneratedId(findMaxId(idColumnName, this.tableName));
            this.generateLoadInfileSql(objectLoader);
        }

        return objectLoader;
    }

    /**
     * @throws StackOverflowError if there is an infinite loop in the object graph for {@link Embedded} fields
     */
    private void findAnnotations(SingleInfileObjectLoader<E> objectLoader) {
        // Finds all columns that are annotated with @Column
        for (PersistenceAnnotationInspector.AnnotatedMethod<Column> annotatedMethod
                : this.annotationInspector.annotatedMethodsWith(this.aClass, Column.class)) {

            Preconditions.checkState(!annotatedMethod.getAnnotation().name().isEmpty(),
                                     "@Column.name is not found on method [%s]",
                                     annotatedMethod.getMethod());
            Column column = annotatedMethod.getAnnotation();
            if (this.secondaryTable != null) {
                if (column.table().equals(this.tableName)) {
                    objectLoader.mappings.put(annotatedMethod.getAnnotation().name(), annotatedMethod.getMethod());
                }
            }
            else if (column.table().isEmpty() || column.table().equals(this.tableName)) {
                objectLoader.mappings.put(annotatedMethod.getAnnotation().name(), annotatedMethod.getMethod());
            }
        }

        // Ignore all these when using secondary table
        if (this.secondaryTable == null) {
            // Finds all one to one columns with @OneToOne
            // Finds all columns with @ManyToOne
            // If @JoinColumn is not there then there is nothing to write
            for (PersistenceAnnotationInspector.AnnotatedMethod<JoinColumn> annotatedMethod
                    : this.annotationInspector.annotatedMethodsWith(this.aClass, JoinColumn.class)) {
                if (this.annotationInspector.hasAnnotation(annotatedMethod.getMethod(), ManyToOne.class)
                    || this.annotationInspector.hasAnnotation(annotatedMethod.getMethod(), OneToOne.class)) {
                    objectLoader.mappings.put(annotatedMethod.getAnnotation().name(), annotatedMethod.getMethod());
                }
            }
            // Finds all columns with @Embedded or @EmbeddedId
            for (PersistenceAnnotationInspector.AnnotatedMethod<? extends Annotation> annotatedMethod
                    : Iterables.concat(this.annotationInspector.annotatedMethodsWith(this.aClass, Embedded.class),
                    this.annotationInspector.annotatedMethodsWith(this.aClass, EmbeddedId.class))) {
                Method method = annotatedMethod.getMethod();
                SingleInfileObjectLoader<Object> embeddedObjectLoader
                        = new SingleInfileObjectLoaderBuilder<>(method.getReturnType(), this.eventBus)
                        .withBuffer(this.infileDataBuffer)
                        .withDefaultTableName()
                        .withStatementExecutor(this.statementExecutor)
                        .withTableName(this.tableName)
                        .usingAnnotationInspector(this.annotationInspector)
                        .allowNull()
                        .isEmbedded()
                        .build();
                objectLoader.embeds.put(method, embeddedObjectLoader);
            }
        }
    }

    private String findPrimaryIdColumnName(SingleInfileObjectLoader<E> objectLoader) {
        Method primaryIdGetter = this.annotationInspector.idGetter(this.aClass);
        if (primaryIdGetter != null) {
            Column column = this.annotationInspector.findAnnotation(primaryIdGetter, Column.class);
            String name = this.annotationInspector.fieldFromGetter(primaryIdGetter).getName();
            if (this.secondaryTable != null) {
                PrimaryKeyJoinColumn[] primaryKeyJoinColumns = this.secondaryTable.pkJoinColumns();
                Preconditions.checkState(primaryKeyJoinColumns.length == 1, "There needs to be one pkJoinColumns");
                name = primaryKeyJoinColumns[0].name();
            }
            else if (column != null && !column.name().isEmpty()) {
                name = column.name();
            }
            objectLoader.mappings.put(name, primaryIdGetter);
            GeneratedValue generatedValue = this.annotationInspector.findAnnotation(primaryIdGetter, GeneratedValue.class);
            objectLoader.autoGenerateId = this.secondaryTable == null
                                          && generatedValue != null
                                          && generatedValue.strategy() == GenerationType.AUTO;
            return name;
        }
        return null;
    }

    private void generateLoadInfileSql(SingleInfileObjectLoader<E> objectLoader) {
        StringBuilder builder = new StringBuilder("LOAD DATA LOCAL INFILE 'stream' ");
        builder.append(this.useReplace ? "REPLACE " : "");
        builder.append("INTO TABLE ");
        builder.append(this.tableName).append(" (");

        ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> setClausesBuilder = ImmutableList.builder();

        populateColumns(objectLoader, columnsBuilder, setClausesBuilder);

        Collection<String> setClauses = setClausesBuilder.build();
        Collection<String> columns = columnsBuilder.build();

        Joiner joiner = Joiner.on(",");
        builder.append(joiner.join(columns)).append(") ");

        // If we had any hex columns then append here
        if (!setClauses.isEmpty()) {
            builder.append("SET ");
            joiner.appendTo(builder, setClauses);
        }

        objectLoader.loadInfileSql = builder.toString();
    }

    /**
     * Find and populate the columns to be inserted. Columns that need to be set are {@code byte[]} fields because they need to
     * be unhexed which is not done when calling {@link InfileDataBuffer#append(byte[])}.
     * <br/>
     * {@link com.opower.persistence.jpile.loader.SingleInfileObjectLoader#getAllColumns()} can not be used since the type
     * of the column is needed to determine if it needs be unhexed.
     * <br/>
     * The {@link ImmutableList.Builder} parameters are modified where the columns are added to them. All the columns,
     * (including the columns of {@link Embedded} fields) will be added to the parameters.
     *
     * @param objectLoader the object loader containing the columns needing to be updated
     * @param columns the columns builder to append to for columns that are part of the infile sql
     * @param setClauses the set clauses builder to append to for set clauses that are part of the infile sql
     * @param <E> the type for the {@link SingleInfileObjectLoader}
     *
     * @throws StackOverflowError if there is an infinite loop in
     * {@link com.opower.persistence.jpile.loader.SingleInfileObjectLoader#getEmbeds()}
     */
    private static <E> void populateColumns(SingleInfileObjectLoader<E> objectLoader, ImmutableList.Builder<String> columns,
            ImmutableList.Builder<String> setClauses) {

        for (Map.Entry<String, Method> entry : objectLoader.getMappings().entrySet()) {
            String column = entry.getKey();

            Method method = entry.getValue();
            Class<?> type = method.getReturnType();

            if (type.isArray() && type.getComponentType() == byte.class) {
                setClauses.add(String.format("%1$s=unhex(@hex%1$s)", column));
                column = "@hex" + column;
            }

            columns.add(column);
        }

        for (SingleInfileObjectLoader<Object> embeddedLoader : objectLoader.getEmbeds().values()) {
            populateColumns(embeddedLoader, columns, setClauses);
        }
    }

    /*
     * Find the max value of the id column in this table. Used when we are attempting to persist an entity into a table
     * that is not empty.
     */
    private long findMaxId(String idColumnName, final String tableName) {
        return this.statementExecutor.execute(new FindMaxIdStatementCallback(idColumnName, tableName));
    }
}
