package datastorm.eventstore.otientdb;

import org.junit.Before;
import org.junit.Ignore;

import java.lang.reflect.Method;

/**
 * Unit test for {@link ODatabaseDocumentStaticProxy}.
 *
 * @author EniSh
 *         Date: 10.04.11
 */
@Ignore
public class ODatabaseDocumentStaticProxyTest extends AbstractODatabaseDocumentProxyTest {
    public ODatabaseDocumentStaticProxyTest(Method testedMethod) {
        super(testedMethod);
    }

    @Before
    public void setUp() {
        super.setUp();
        proxy = new ODatabaseDocumentStaticProxy(databaseFactoryMock);
    }
}
