package ua.com.datastorm.spring;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import ua.com.datastorm.eventstore.orientdb.ConnectionManager;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.FactoryBeanNotInitializedException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author EniSh
 */
public class ODatabaseDocumentFactoryBean implements FactoryBean<ODatabaseDocument> {
    private ConnectionManager connectionManager;
    private ODatabaseDocument oDatabaseDocumentProxy;

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        oDatabaseDocumentProxy = (ODatabaseDocument) Proxy.newProxyInstance(ODatabaseDocument.class.getClassLoader(),
                new Class[]{ODatabaseDocument.class},
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        ODatabaseDocument connection =
                                (ODatabaseDocument) TransactionSynchronizationManager.getResource(
                                        ODatabaseDocumentFactoryBean.this.connectionManager);

                        if (connection == null) {
                            throw new IllegalStateException("Can't be invoked in non transactional scope");
                        }

                        return method.invoke(connection, args);
                    }
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ODatabaseDocument getObject() throws Exception {
        if (connectionManager == null) {
            throw new FactoryBeanNotInitializedException("Parameter connection manager can't be null");
        }

        return oDatabaseDocumentProxy;
    }

    /**
     * <p> Always returns {@link ODatabaseDocument}. </p>
     *
     * @return {@link ODatabaseDocument}
     * @see org.springframework.beans.factory.FactoryBean#getObjectType()
     */
    @Override
    public Class<?> getObjectType() {
        return ODatabaseDocument.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
