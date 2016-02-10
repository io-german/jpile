package com.opower.persistence.jpile.loader;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.opower.persistence.jpile.infile.InfileDataBuffer;
import com.opower.persistence.jpile.infile.events.EventFirePoint;
import com.opower.persistence.jpile.infile.events.SaveEntityEvent;
import com.opower.persistence.jpile.infile.events.SaveEntityEventAdapter;
import com.opower.persistence.jpile.reflection.CachedProxy;
import com.opower.persistence.jpile.reflection.PersistenceAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;

import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.SecondaryTable;
import javax.sql.DataSource;
import java.io.Closeable;
import java.io.Flushable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Save any type of data using a collection of SingleInfileObjectLoaders. A common use case would be to do something like
 * <p/>
 * <pre>
 *     Connection connection = ...
 *     HierarchicalInfileObjectLoader objectLoader = new HierarchicalInfileObjectLoader();
 *     objectLoader.setConnection(connection);
 *     try {
 *         objectLoader.persist(foo, bar);
 *     } finally {
 *         objectLoader.close();
 *         connection.close();
 *     }
 * </pre>
 * Note that because the connection is passed in, it is up to the caller to close the connection correctly. Otherwise the
 * connection will never be closed.
 *
 * @author amir.raminfar
 * @since 1.0
 */
public class HierarchicalInfileObjectLoader implements Flushable, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HierarchicalInfileObjectLoader.class);
    private static final String EVENT_BUS_IDENTIFIER = "jpile-event-bus";

    private PersistenceAnnotationInspector persistenceAnnotationInspector =
            CachedProxy.create(new PersistenceAnnotationInspector());

    private EventBus eventBus = new EventBus(EVENT_BUS_IDENTIFIER);
    private ConnectionHolder connectionHolder;
    private RetryPolicy retryPolicy = new NeverRetryPolicy();

    // linked for consistent error message
    private Map<Class<?>, SingleInfileObjectLoader<Object>> primaryObjectLoaders = newLinkedHashMap();
    private Map<Class<?>, SingleInfileObjectLoader<Object>> secondaryTableObjectLoaders = newLinkedHashMap();
    private Map<Class<?>, Set<Method>> parentDependent = newHashMap();
    private Map<Class<?>, Set<Method>> childDependent = newHashMap();
    private Set<Class<?>> classesToIgnore = ImmutableSet.of();
    private Set<String> secondaryClassesToIgnore = ImmutableSet.of();
    private boolean useReplace = false;

    /**
     * Disables fk (if not already disabled) and saves each object
     *
     * @param firstObject the first object to save
     * @param moreObjects optional more objects
     */
    public void persist(Object firstObject, Object... moreObjects) {
        persist(concat(ImmutableList.of(firstObject), ImmutableList.copyOf(moreObjects)));
    }

    /**
     * Disables fk (if not already disabled) and saves each object
     *
     * @param objects the objects to save
     */
    public void persist(Iterable<?> objects) {
        Preconditions.checkNotNull(
                this.connectionHolder, "ConnectionHolder is null, did you call setConnection() or setDataSource()?");

        for (Object o : objects) {
            persistWithCyclicCheck(o, new HashSet<>());
        }
    }

    private void persistWithCyclicCheck(Object entity, Set<Object> cyclicCheck) {
        Preconditions.checkNotNull(entity, "Cannot persist null");

        // If we already saved this object then ignore
        if (cyclicCheck.contains(entity)) {
            LOGGER.debug("Skipping in file persist on [{}] because it has already been saved.", entity);
            return;
        }

        // If we are supposed to ignore this class then also ignore
        if (this.classesToIgnore.contains(entity.getClass())) {
            LOGGER.debug("Ignoring [{}].", entity);
            return;
        }

        LOGGER.debug("Persisting [{}].", entity);

        // Initialize for this class
        initForClass(entity.getClass());

        // Add to a set so we don't save this object again
        cyclicCheck.add(entity);

        // Save dependent children first because there is a key that depends on these items
        for (Method dependent : this.childDependent.get(entity.getClass())) {
            Object o = invoke(dependent, entity);
            if (o != null) {
                persistWithCyclicCheck(o, cyclicCheck);
            }
        }

        // Save this entity now that we know all children have been saved
        this.eventBus.post(new SaveEntityEvent(this, EventFirePoint.BEFORE, entity));
        this.primaryObjectLoaders.get(entity.getClass()).add(entity);
        this.eventBus.post(new SaveEntityEvent(this, EventFirePoint.AFTER, entity));

        // Get generated id
        Object id = PersistenceAnnotationInspector.getIdValue(this.persistenceAnnotationInspector, entity);

        // Find all objects that depend entity's id being generated and save these now
        for (Method dependent : this.parentDependent.get(entity.getClass())) {
            Object o = invoke(dependent, entity);
            if (o != null) {
                if (o instanceof Collection) {
                    for (Object item : (Collection) o) {
                        persistWithCyclicCheck(item, cyclicCheck);
                    }
                }
                else {
                    PersistenceAnnotationInspector.setIdValue(this.persistenceAnnotationInspector, o, id);
                    persistWithCyclicCheck(o, cyclicCheck);
                }
            }
        }

        // Check to see if there is a secondary
        SingleInfileObjectLoader<Object> loader = this.secondaryTableObjectLoaders.get(entity.getClass());
        if (loader != null) {
            loader.add(entity);
        }
    }

    private void initForClass(Class<?> aClass) {
        findParentDependents(aClass);
        findChildDependents(aClass);
        createObjectLoader(aClass);
    }

    private void createObjectLoader(Class<?> aClass) {
        if (this.primaryObjectLoaders.containsKey(aClass)) {
            return;
        }
        SingleInfileObjectLoader<Object> primaryLoader = new SingleInfileObjectLoaderBuilder<>(aClass, this.eventBus)
                .withBuffer(newInfileDataBuffer())
                .withDefaultTableName()
                .withConnectionHolder(this.connectionHolder)
                .withRetryPolicy(this.retryPolicy)
                .usingAnnotationInspector(this.persistenceAnnotationInspector)
                .useReplace(this.useReplace)
                .build();

        this.primaryObjectLoaders.put(aClass, primaryLoader);

        for (SecondaryTable secondaryTable : this.persistenceAnnotationInspector.findSecondaryTables(aClass)) {
            if (!this.secondaryClassesToIgnore.contains(secondaryTable.name())) {
                SingleInfileObjectLoader<Object> secondaryLoader
                        = new SingleInfileObjectLoaderBuilder<>(aClass, this.eventBus)
                        .withBuffer(newInfileDataBuffer())
                        .withDefaultTableName()
                        .usingSecondaryTable(secondaryTable)
                        .withConnectionHolder(this.connectionHolder)
                        .withRetryPolicy(this.retryPolicy)
                        .usingAnnotationInspector(this.persistenceAnnotationInspector)
                        .useReplace(this.useReplace)
                        .build();

                this.secondaryTableObjectLoaders.put(aClass, secondaryLoader);
            }
        }
    }

    private void findParentDependents(Class<?> aClass) {
        if (this.parentDependent.containsKey(aClass)) {
            return;
        }
        Set<Method> methods = newHashSet(this.persistenceAnnotationInspector.methodsAnnotatedWith(aClass, OneToMany.class));
        methods.addAll(this.persistenceAnnotationInspector.methodsAnnotatedWith(aClass, OneToOne.class,
                                                                           PrimaryKeyJoinColumn.class));
        this.parentDependent.put(aClass, methods.size() > 0 ? methods : ImmutableSet.<Method>of());

        // Do all children again
        for (Method m : methods) {
            findParentDependents(getReturnType(m));
        }
    }

    private void findChildDependents(Class<?> aClass) {
        if (this.childDependent.containsKey(aClass)) {
            return;
        }

        Set<Method> methods = newHashSet(this.persistenceAnnotationInspector.methodsAnnotatedWith(aClass, ManyToOne.class));
        methods.addAll(this.persistenceAnnotationInspector.methodsAnnotatedWith(aClass, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                // Must have OneToOne but not PrimaryKeyJoinColumn annotations
                PersistenceAnnotationInspector annotationInspector =
                        HierarchicalInfileObjectLoader.this.persistenceAnnotationInspector;
                return annotationInspector.hasAnnotation(m, OneToOne.class)
                        && !annotationInspector.hasAnnotation(m, PrimaryKeyJoinColumn.class);
            }
        }));

        this.childDependent.put(aClass, methods.size() > 0 ? methods : ImmutableSet.<Method>of());

        // Do all children again
        for (Method m : methods) {
            findChildDependents(getReturnType(m));
        }
    }

    private Class<?> getReturnType(Method m) {
        if (m.getGenericReturnType() instanceof ParameterizedType) {
            // For List<String> etc...
            ParameterizedType type = (ParameterizedType) m.getGenericReturnType();
            return (Class<?>) type.getActualTypeArguments()[0];
        }
        else {
            return (Class<?>) m.getGenericReturnType();
        }
    }

    private InfileDataBuffer newInfileDataBuffer() {
        return new InfileDataBuffer();
    }

    private Object invoke(Method method, Object target) {
        try {
            return method.invoke(target);
        }
        catch (ReflectiveOperationException e) {
            throw propagate(e);
        }
    }

    /**
     * Flushes all object loaders
     */
    @Override
    public void flush() {
        LOGGER.debug("Flushing all object loaders.");
        for (SingleInfileObjectLoader<?> loader : this.primaryObjectLoaders.values()) {
            loader.flush();
        }
        for (SingleInfileObjectLoader<?> loader : this.secondaryTableObjectLoaders.values()) {
            loader.flush();
        }
    }

    /**
     * Closes all object loaders.
     * <p/>
     * Re-enables foreign key checks for the connection.
     */
    @Override
    public void close() {
        flush();
        LOGGER.debug("Closing all object loaders.");
        this.primaryObjectLoaders.clear();
        this.secondaryTableObjectLoaders.clear();
        this.connectionHolder.resetCurrentConnection();
    }

    public void setConnection(Connection connection) {
        Preconditions.checkNotNull(connection, "connection can't be null");
        this.connectionHolder = new ConnectionHolder(connection);
    }

    public void setDataSource(DataSource dataSource) {
        Preconditions.checkNotNull(dataSource, "dataSource can't be null");
        this.connectionHolder = new ConnectionHolder(dataSource);
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        Preconditions.checkNotNull(retryPolicy, "retryPolicy can't be null");
        this.retryPolicy = retryPolicy;
    }

    /**
     * This method is deprecated.
     *
     * @param classToIgnore classes to ignore
     * @deprecated due to improper type handling; use {@link #setIgnoredClasses(Set)} instead.
     */
    @SuppressWarnings("rawtypes") // For legacy compatibility
    @Deprecated
    public void setClassesToIgnore(Set<Class> classToIgnore) {
        ImmutableSet.Builder<Class<?>> typedClassesToIgnore = ImmutableSet.builder();
        for (Class untypedClass : classToIgnore) {
            typedClassesToIgnore.add(untypedClass);
        }
        setIgnoredClasses(typedClassesToIgnore.build());
    }

    /**
     * Set the classes to ignore.
     * @param classesToIgnore classes to ignore
     * @since 1.8.0
     */
    public void setIgnoredClasses(Set<Class<?>> classesToIgnore) {
        this.classesToIgnore = classesToIgnore;
    }

    public void setSecondaryClassesToIgnore(Set<String> secondaryClassesToIgnore) {
        this.secondaryClassesToIgnore = secondaryClassesToIgnore;
    }

    /**
     * This method is decommissioned.
     * <br/>
     * Use {@link #subscribe(Object)} method by passing listener object which has public method that
     * marked by {@link com.google.common.eventbus.Subscribe} annotation and accepts {@link SaveEntityEvent}
     * as argument. Example:
     * <pre>
     * {@code public class Listener {
     *     @literal @Subscribe public void handle(SaveEntityEvent event) { }
     *  }}
     * </pre>
     *
     * @see #subscribe
     * @see #unsubscribe
     *
     * @deprecated
     */
    @Deprecated
    public void setEventCallback(CallBack eventCallback) {
        subscribe(new SaveEntityEventAdapter(eventCallback));
    }

    /**
     * Subscribes listener to the events stream.
     * <br/>
     * Listener object should have public method that marked by {@link com.google.common.eventbus.Subscribe} annotation
     * and accepts needed event type as argument. Example:
     * <pre>
     * {@code public class Listener {
     *     @literal @Subscribe public void handle(SaveEntityEvent event) { }
     *     @literal @Subscribe public void handle(FlushEvent event) { }
     *  }}
     * </pre>
     *
     * @see EventBus#register(Object)
     * @since 1.8.0
     */
    public void subscribe(Object listener) {
        this.eventBus.register(listener);
    }

    /**
     * Un-subscribes listener from the events stream.
     *
     * @see EventBus#unregister(Object)
     * @since 1.8.0
     */
    public void unsubscribe(Object listener) {
        this.eventBus.unregister(listener);
    }

    /**
     * Sets event bus implementation.
     *
     * @see com.google.common.eventbus.EventBus
     * @see com.google.common.eventbus.AsyncEventBus
     *
     * @since 1.8.0
     */
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Toggles the {@code REPLACE} option. Using {@code REPLACE} causes input rows to replace existing rows for rows that have the
     * same value for a primary key or unique index as an existing row.
     */
    public void setUseReplace(boolean useReplace) {
        this.useReplace = useReplace;
    }

    /**
     * An event interface that can be used to do perform actions before and after persisting objects
     *
     * @deprecated
     */
    @Deprecated
    public interface CallBack {
        /**
         * Gets called before saving an object
         *
         * @param o the object
         */
        void onBeforeSave(Object o);

        /**
         * Gets called after the object has been saved
         *
         * @param o the object
         */
        void onAfterSave(Object o);
    }
}
