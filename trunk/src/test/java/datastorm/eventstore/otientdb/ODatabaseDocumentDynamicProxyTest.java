package datastorm.eventstore.otientdb;

import org.junit.Before;

import java.lang.reflect.Method;

/**
 * Unit test for {@link ODatabaseDocumentDynamicProxyFactory}.
 *
 * @author EniSh
 *         Date: 10.04.11
 */
public class ODatabaseDocumentDynamicProxyTest extends AbstractODatabaseDocumentProxyTest {
    public ODatabaseDocumentDynamicProxyTest(Method testedMethod) {
        super(testedMethod);
    }

    @Before
    public void setUp() {
        super.setUp();
        proxy = new ODatabaseDocumentDynamicProxyFactory().getInstance(databaseFactoryMock);
    }
}
