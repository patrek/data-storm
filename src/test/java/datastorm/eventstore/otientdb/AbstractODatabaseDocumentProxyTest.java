package datastorm.eventstore.otientdb;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * <p> Abstract unit test for database proxies. It tests that each method delegates to database
 * connection provided by {@link ThreadedODatabaseDocumentFactory}. </p>
 * <p> Childes have to initialize {@link AbstractODatabaseDocumentProxyTest#proxy} by specific
 * proxy implementation. </p>
 *
 * @author EniSh
 *         Date: 07.04.11
 */
@RunWith(value = Parameterized.class)
public abstract class AbstractODatabaseDocumentProxyTest {

    private Method testedMethod;
    private ODatabaseDocument proxiedObjectMock;

    /**
     * Mock for object that provide connection to database.
     */
    protected ThreadedODatabaseDocumentFactory databaseFactoryMock;

    /**
     * Object which we test for correct method delegation.
     */
    protected ODatabaseDocument proxy;

    public AbstractODatabaseDocumentProxyTest(Method testedMethod) {
        this.testedMethod = testedMethod;
    }

    @Before
    public void setUp() {
        databaseFactoryMock = mock(ThreadedODatabaseDocumentFactory.class);
        proxiedObjectMock = mock(ODatabaseDocument.class);
    }

    @Test
    public void testMethodDelegation() {
        try {

            Object[] args = prepareMethodArguments();

            when(databaseFactoryMock.getThreadLocalDatabase()).thenReturn(proxiedObjectMock);

            if (testedMethod.getReturnType().equals(void.class)) {
                testMethodWithoutResult(args);
            } else {
                testMethodWithResult(args);
            }

            testedMethod.invoke(verify(proxiedObjectMock), args);
            verify(databaseFactoryMock).getThreadLocalDatabase();
        } catch (IllegalAccessException e) {
            fail("Can't access method " + testedMethod.getName());
        } catch (InvocationTargetException e) {
            fail("Unexpected exception " + e.getTargetException());
        }
    }

    private void testMethodWithoutResult(Object[] args) throws IllegalAccessException, InvocationTargetException {
        testedMethod.invoke(doNothing().when(proxiedObjectMock), args);
        testedMethod.invoke(proxy, args);
    }

    private void testMethodWithResult(Object[] args) throws IllegalAccessException, InvocationTargetException {
        Object expectedResult = prepareExpectedResult();
        when(testedMethod.invoke(proxiedObjectMock, args)).thenReturn(expectedResult);

        Object result = testedMethod.invoke(proxy, args);

        assertEquals("Result method invocation in proxy is not same as in proxied object", expectedResult, result);
    }

    private Object[] prepareMethodArguments() {
        Class<?>[] parameterTypes = testedMethod.getParameterTypes();
        Object[] objects = new Object[parameterTypes.length];
        for (int i = 0; i < objects.length; i++) {
            if (parameterTypes[i].isPrimitive()) {
                objects[i] = preparePrimitiveObject(parameterTypes[i]);
            }
        }
        return objects;
    }

    private Object prepareExpectedResult() {
        Class<?> returnType = testedMethod.getReturnType();
        if (returnType.equals(Class.class)) {
            return Object.class;
        } else if (returnType.isPrimitive()) {
            return preparePrimitiveObject(returnType);
        } else if (returnType.equals(String.class)) {
            return "";
        } else {
            return mock(returnType);
        }
    }

    private Object preparePrimitiveObject(Class<?> returnType) {
        if (returnType.equals(boolean.class)) {
            return true;
        } else if (returnType.equals(int.class)) {
            return 0;
        } else if (returnType.equals(long.class)) {
            return 0l;
        } else if (returnType.equals(double.class)) {
            return 0.0;
        } else if (returnType.equals(float.class)) {
            return 0.0f;
        } else {
            throw new UnsupportedOperationException(returnType.getName() + " unsupported by test yet.");
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {

        ArrayList<Object[]> parameters = new ArrayList<Object[]>();
        parameters.addAll(Collections2.transform(Arrays.asList(ODatabaseDocument.class.getMethods()), new Function<Object, Object[]>() {
            @Override
            public Object[] apply(Object o) {
                return new Object[]{o};
            }
        }));
        return parameters;
    }
}
