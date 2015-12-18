package com.opower.persistence.jpile.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import javax.persistence.Id;
import javax.persistence.SecondaryTable;
import javax.persistence.SecondaryTables;
import javax.persistence.Table;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;

/**
 * The default implementation which parses the annotations. This class should be used with {@link CachedProxy} to cache
 * the reflection calls. Without caching, there is a huge performance loss.
 *
 * @author amir.raminfar
 * @since 1.0
 */
public class PersistenceAnnotationInspector {
    private static final String SETTER_PREFIX = "set";
    private static final String GETTER_PREFIX = "get";
    private static final String IS_PREFIX = "is";

    /**
     * Finds the annotation on a class or subclasses.
     * Uses <code>AnnotationUtils.findAnnotation()</code> from Spring framework. Searches all subclasses and class.
     *
     * @param clazz          the class to look
     * @param annotationType the annotation type
     * @param <A>            the annotation
     * @return return the annotation or null if it doesn't exist
     */
    public <A extends Annotation> A findAnnotation(Class<?> clazz, Class<A> annotationType) {
        return AnnotationUtils.findAnnotation(clazz, annotationType);
    }

    /**
     * Finds the annotation on a method or parent's methods.
     * Uses <code>AnnotationUtils.findAnnotation()</code> from Spring framework. Searches all subclasses and class.
     *
     * @param method         the method to look
     * @param annotationType the annotation class
     * @param <A>            the annotation type
     * @return the annotation on the method or null if it doesn't exist
     */
    public <A extends Annotation> A findAnnotation(Method method, Class<A> annotationType) {
        return AnnotationUtils.findAnnotation(method, annotationType);
    }

    /**
     * Checks to see if an annotation exists on a method.
     *
     * @param method         the method
     * @param annotationType the annotation to look for
     * @return true if it exists
     */
    public boolean hasAnnotation(Method method, Class<? extends Annotation> annotationType) {
        return findAnnotation(method, annotationType) != null;
    }

    /**
     * Checks to see if an annotation exists on a class.
     *
     * @param clazz          the class to look for
     * @param annotationType the annotation class
     * @return true if it exists
     */
    public boolean hasAnnotation(Class<?> clazz, Class<? extends Annotation> annotationType) {
        return findAnnotation(clazz, annotationType) != null;
    }

    /**
     * Gets the table name for the {@link javax.persistence.Table &#064;Table} annotation on a class.
     *
     * @param aClass the class to look
     * @return the table name, null if it doesn't exist
     */
    public String tableName(Class<?> aClass) {
        Table table = findAnnotation(aClass, Table.class);
        Preconditions.checkNotNull(table, "@Table annotation not found for class [%s]", aClass);
        if (table.name().isEmpty()) {
            return aClass.getSimpleName().toLowerCase();
        }
        else {
            return table.name();
        }
    }

    /**
     * Checks to see if {@link javax.persistence.Table &#064;Table} exist.
     *
     * @param aClass the class to search
     * @return true if @Table exists on class
     */
    public boolean hasTableAnnotation(Class<?> aClass) {
        return hasAnnotation(aClass, Table.class);
    }

    /**
     * Look for {@link javax.persistence.SecondaryTable &#064;SecondaryTable} annotations and return the name.
     *
     * @param aClass the class to look for
     * @return the table name or null if it doesn't exist
     */
    public String secondaryTable(Class<?> aClass) {
        SecondaryTable table = findAnnotation(aClass, SecondaryTable.class);
        if (table != null) {
            return table.name();
        }
        return null;
    }

    /**
     * Parses and returns all SecondaryTables on a class. This includes {@link javax.persistence.SecondaryTable &#064;
     * SecondaryTable}
     * and {@link javax.persistence.SecondaryTables &#064;SecondaryTables}. If no annotations are found then
     * an empty list is returned.
     *
     * @param aClass the class to search
     * @return collection of secondary table annotations, empty if none found
     */
    public List<SecondaryTable> findSecondaryTables(Class<?> aClass) {
        List<SecondaryTable> annotations = ImmutableList.of();
        SecondaryTable secondaryTable = findAnnotation(aClass, SecondaryTable.class);
        SecondaryTables secondaryTables = findAnnotation(aClass, SecondaryTables.class);
        if (secondaryTables != null) {
            annotations = copyOf(secondaryTables.value());
        }
        else if (secondaryTable != null) {
            annotations = of(secondaryTable);
        }
        return annotations;
    }

    /**
     * Looks for {@link javax.persistence.Id &#064;Id} on all methods and returns the getter.
     *
     * @param aClass the class to look for
     * @return the getter method
     */
    public Method idGetter(Class<?> aClass) {
        List<AnnotatedMethod<Id>> methods = annotatedMethodsWith(aClass, Id.class);
        return methods.size() > 0 ? methods.get(0).getMethod() : null;
    }

    /**
     * Check if a class is a boolean type.
     *
     * @param aClass the class to check
     * @return true if a {@link Boolean} or {@link boolean}
     */
    private static boolean isBooleanClass(Class<?> aClass) {
        return Boolean.class.equals(aClass) || boolean.class.equals(aClass);
    }

    /**
     * Get the getter prefix for a getter method,
     *
     * @param getter the getter method
     * @return the prefix, either 'is' or 'get'
     */
    private static String getGetterPrefix(Method getter) {
        if (isBooleanClass(getter.getReturnType()) && getter.getName().startsWith(IS_PREFIX)) {
            return IS_PREFIX;
        }

        return GETTER_PREFIX;
    }

    /**
     * Check getter preconditions to ensure that method is really a getter.
     *
     * @param getter the getter method
     */
    private static void checkGetterPreconditions(Method getter) {
        Preconditions.checkNotNull(getter.getReturnType(), "Getter must have a return type");
        Preconditions.checkState(getter.getParameterTypes().length == 0, "Getter must not have any parameters");
        Preconditions.checkState(getter.getName().startsWith(GETTER_PREFIX)
                || (isBooleanClass(getter.getReturnType()) && getter.getName().startsWith(IS_PREFIX)),
                "Getter must start with %s or %s if a Boolean/boolean return type", GETTER_PREFIX, IS_PREFIX);
    }

    /**
     * Finds the setter for a getter by following the JavaBean naming convention, replacing 'is'/'get' with 'set'.
     *
     * @param getter the getter
     * @return the setter if it exists, otherwise null
     */
    public Method setterFromGetter(Method getter) {
        Preconditions.checkNotNull(getter, "Cannot find setter from null getter");
        checkGetterPreconditions(getter);

        Class<?> aClass = getter.getDeclaringClass();
        String getterPrefix = getGetterPrefix(getter);

        String setterName = getter.getName().replaceFirst(getterPrefix, SETTER_PREFIX);
        return ReflectionUtils.findMethod(aClass, setterName, getter.getReturnType());
    }

    /**
     * Finds the getter for a setter by following the JavaBean naming convention, replacing 'set' with 'get'/'is'.
     *
     * @param setter the setter
     * @return the getter if it exists, otherwise null
     */
    public Method getterFromSetter(Method setter) {
        Preconditions.checkNotNull(setter, "Cannot find getter from null setter");
        Preconditions.checkState(void.class.equals(setter.getReturnType()), "Setter cannot have return type");
        Preconditions.checkState(setter.getParameterTypes().length == 1, "Setter must have just one parameter");
        Preconditions.checkState(setter.getName().startsWith(SETTER_PREFIX), "Setter must start with %s", SETTER_PREFIX);

        Class<?> aClass = setter.getDeclaringClass();

        Method getter = ReflectionUtils.findMethod(aClass, setter.getName().replaceFirst(SETTER_PREFIX, GETTER_PREFIX));

        if (getter == null && setter.getParameterTypes().length == 1 && isBooleanClass(setter.getParameterTypes()[0])) {
            // Retry to find the getter since Boolean/boolean getter methods also start with 'is' instead of 'get'.
            getter = ReflectionUtils.findMethod(aClass, setter.getName().replaceFirst(SETTER_PREFIX, IS_PREFIX));
        }
        return getter;
    }

    /**
     * Searches for field that matches the getter following JavaBean naming conventions (eg 'getXyz'/'isXyz' corresponds with
     * the field 'xyz'/'isXyz').
     *
     * @param getter the getter
     * @return the field or null
     */
    public Field fieldFromGetter(Method getter) {
        Preconditions.checkNotNull(getter, "Cannot find field from null getter");
        checkGetterPreconditions(getter);
        Class<?> aClass = getter.getDeclaringClass();

        String getterName = getter.getName();
        String getterPrefix = getGetterPrefix(getter);

        String getterNameWithoutPrefix = getterName.replaceFirst(getterPrefix, "");
        String fieldName = Character.toLowerCase(getterNameWithoutPrefix.charAt(0)) + getterNameWithoutPrefix.substring(1);
        Field field = ReflectionUtils.findField(aClass, fieldName, getter.getReturnType());

        if (field == null && isBooleanClass(getter.getReturnType())) {
            // Retry to find the field since boolean/Boolean type variables also start with 'is'.
            fieldName = IS_PREFIX + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
            field = ReflectionUtils.findField(aClass, fieldName, getter.getReturnType());
        }

        return field;
    }

    /**
     * Looks for all methods with an annotation and returns the annotation with the method.
     *
     * @param aClass     the class
     * @param annotation the annotation class
     * @param <A>        the annotation type
     * @return list of annotations and methods together
     */
    public <A extends Annotation> List<AnnotatedMethod<A>> annotatedMethodsWith(Class<?> aClass, Class<A> annotation) {
        List<AnnotatedMethod<A>> methods = newArrayList();
        for (Method m : ReflectionUtils.getAllDeclaredMethods(aClass)) {
            A a = m.getAnnotation(annotation);
            if (a != null) {
                methods.add(new AnnotatedMethod<>(m, a));
            }
        }

        return methods;
    }

    /**
     * Returns all methods that are annotated with multiple annotations.
     *
     * @param aClazz      the class to search
     * @param annotations all annotations
     * @return the list of methods
     */
    @SafeVarargs
    public final List<Method> methodsAnnotatedWith(Class<?> aClazz, final Class<? extends Annotation>... annotations) {
        return methodsAnnotatedWith(aClazz, new Predicate<Method>() {

            public boolean apply(Method m) {
                for (Class<? extends Annotation> annotation : annotations) {
                    if (m.getAnnotation(annotation) == null) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    /**
     * Returns all methods filtered by a predicate.
     *
     * @param aClass    the class to search
     * @param predicate using this predicate to filter
     * @return the list of methods
     */
    public List<Method> methodsAnnotatedWith(Class<?> aClass, Predicate<Method> predicate) {
        return newArrayList(Iterables.filter(copyOf(ReflectionUtils.getAllDeclaredMethods(aClass)), predicate));
    }

    /**
     * A helper method for getting an id from a persist object with annotated @Id.
     * <p/>
     * The reason is static and takes an instance of self for caching reasons. @Cacheable does not work when calling
     * <code>this.someCachedMethod()</code> so I am passing the object instead which caches everything.
     *
     * @param utils an instance of this class or sub-class
     * @param o     the object
     * @return the id
     */
    public static Object getIdValue(PersistenceAnnotationInspector utils, Object o) {
        Preconditions.checkNotNull(o, "Cannot get id on a null object");
        if (utils.hasTableAnnotation(o.getClass())) {
            Method getter = utils.idGetter(o.getClass());
            if (getter != null) {
                try {
                    return getter.invoke(o);
                }
                catch (InvocationTargetException | IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        return null;
    }

    /**
     * Sets the value by find a getter with @Id and the setter that goes with that field. If a setter doesn't exist
     * then it falls back looking for the field.
     *
     * The reason is static and takes an instance of self for caching reasons. @Cacheable does not work when calling
     * <code>this.someCachedMethod()</code> so I am passing the object instead which caches everything.
     *
     * @param utils  an instance of this class or sub-class
     * @param entity the object
     * @param id     the new value
     */
    public static void setIdValue(PersistenceAnnotationInspector utils, Object entity, Object id) {
        Preconditions.checkNotNull(entity, "Cannot update id on a null object");
        Method getter = utils.idGetter(entity.getClass());
        Method setter = utils.setterFromGetter(getter);
        Field field = utils.fieldFromGetter(getter);

        try {
            if (setter != null) {
                ReflectionUtils.makeAccessible(setter);
                setter.invoke(entity, id);
            }
            else if (field != null) {
                ReflectionUtils.makeAccessible(field);
                field.set(entity, id);
            }
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * For paring annotations and methods.
     *
     * @param <E> the annotation type
     */
    public static class AnnotatedMethod<E extends Annotation> {
        private final Method method;
        private final E annotation;

        public AnnotatedMethod(Method method, E annotation) {
            this.method = method;
            this.annotation = annotation;
        }

        public Method getMethod() {
            return this.method;
        }

        public E getAnnotation() {
            return this.annotation;
        }
    }
}
