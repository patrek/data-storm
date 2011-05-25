package ua.com.datastorm.spring;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * @author Artem Orobets enisher@gmail.com
 */
@RunWith(value = Parameterized.class)
public class OrientPersistenceExceptionTranslatorTest {

    private static final String ERROR_MESSAGE = "ERROR";
    private OrientPersistenceExceptionTranslator exceptionTranslator;

    private RuntimeException inputException;
    private Class<? extends DataAccessException> outputException;

    public OrientPersistenceExceptionTranslatorTest(RuntimeException inputException, Class<? extends DataAccessException> outputException) {
        this.inputException = inputException;
        this.outputException = outputException;
    }

    @Before
    public void setUp() {
        exceptionTranslator = new OrientPersistenceExceptionTranslator();
    }

    @Test
    public void testTranslateExceptionIfPossibleIllegalStateException() throws Exception {
        DataAccessException result = exceptionTranslator.translateExceptionIfPossible(inputException);
        assertEquals((result != null) ? result.getClass() : null, outputException);
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(
                new Object[]{new IllegalStateException(), InvalidDataAccessApiUsageException.class},
                new Object[]{new IllegalArgumentException(), InvalidDataAccessApiUsageException.class},
                new Object[]{new OConcurrentModificationException(ERROR_MESSAGE), OptimisticLockingFailureException.class},
                new Object[]{new ORecordNotFoundException(ERROR_MESSAGE), DataRetrievalFailureException.class},
                new Object[]{new OTransactionException(ERROR_MESSAGE), InvalidDataAccessApiUsageException.class},
                new Object[]{new OException(), OrientSystemException.class}
        );
    }
}
