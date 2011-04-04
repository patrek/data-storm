package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author EniSh
 *         Date: 04.04.11
 */
public class ODatabaseDocumentDynamicProxyFactory {
    public ODatabaseDocument getInstance(final ThreadedODatabaseDocumentFactory databaseFactory) {
        return (ODatabaseDocument) Proxy.newProxyInstance(ODatabaseDocumentTx.class.getClassLoader(),
                ODatabaseDocumentTx.class.getInterfaces(),
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        return method.invoke(databaseFactory.getThreadLocalDatabase(), args);
                    }
                });
    }
}
