package com.opower.persistence.jpile.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

import javax.annotation.Nonnull;

/**
 * Creates an intermediate proxy object for a given interface. All calls are cached. See
 * <a href="https://github.com/martinus/java-playground/blob/master/src/java/com/ankerl/proxy/CachedProxy.java">CachedProxy</a>
 * for the original idea. An example of usage is:
 * <pre>
 *     Foo foo = new Foo();
 *     Foo cachedFoo = CachedProxy.create(foo);
 * </pre>
 *
 * @author Martin Ankerl (martin.ankerl@gmail.at)
 * @author amir.raminfar
 */
public final class CachedProxy {
    private CachedProxy() {
    }

    /**
     * Private class with all the required parameters for a method
     */
    private static final class Args {
        private final Method method;
        private final Object[] args;

        public Args(final Method method, final Object[] args) {
            this.method = method;
            this.args = Arrays.copyOf(args, args.length);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof Args) {
                Args other = (Args) obj;
                return Objects.equal(this.method, other.method) && Arrays.deepEquals(this.args, other.args);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[] { this.method, this.args });
        }
    }

    /**
     * Creates an intermediate proxy object that uses cached results if
     * available, otherwise calls the given code.
     *
     * @param <T>  Type of the class.
     * @param impl The actual implementation code that should be cached.
     * @return The proxy
     */
    public static <T> T create(final T impl) {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(impl.getClass());
        @SuppressWarnings("unchecked")
        Class<? extends T> cachedClass = factory.createClass();
        try {
            T cachedInstance = cachedClass.newInstance();
            ((ProxyObject) cachedInstance).setHandler(new MethodHandler() {
                final LoadingCache<Args, Optional<Object>> cache = createCache(impl);

                /**
                 * Returns the cached value of this method. If the method returns null then null is returned.
                 *
                 * {@inheritDoc}
                 */
                @Override
                public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) {
                    return this.cache.getUnchecked(new Args(thisMethod, args)).orNull();
                }
            });
            return cachedInstance;
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static LoadingCache<Args, Optional<Object>> createCache(final Object impl) {
        return CacheBuilder.newBuilder()
                           .softValues()
                           .build(new CacheLoader<Args, Optional<Object>>() {
                               @Override
                               public Optional<Object> load(@Nonnull Args key)
                                       throws InvocationTargetException, IllegalAccessException {
                                   return Optional.fromNullable(key.method.invoke(impl, key.args));
                               }
                           });
    }
}
