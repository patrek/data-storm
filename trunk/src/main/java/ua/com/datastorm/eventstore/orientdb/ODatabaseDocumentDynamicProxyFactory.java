package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * <p>Factory for creation proxies for {@link ODatabaseDocument) which delegate method
 * invocation to database connection provided by {@link ThreadedODatabaseDocumentFactory}.</p>
 * <p>Implemented using {@link Proxy}.</p>
 *
 * @author EniSh
 *         Date: 04.04.11
 */
public class ODatabaseDocumentDynamicProxyFactory {
    /**
     * <p>Creates instance of {@link ODatabaseDocument} proxy.</p>
     *
     * @param databaseFactory factory which provide database connection
     * @return proxy instance
     */
    public ODatabaseDocument getInstance(final ThreadedODatabaseDocumentFactory databaseFactory) {
        return (ODatabaseDocument) Proxy.newProxyInstance(ODatabaseDocument.class.getClassLoader(),
                new Class[]{ODatabaseDocument.class},
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        return method.invoke(databaseFactory.getThreadLocalDatabase(), args);
                    }
                });
    }
}
