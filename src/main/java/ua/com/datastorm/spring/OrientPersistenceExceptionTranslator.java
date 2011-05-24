package ua.com.datastorm.spring;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Performs exception translation of OrientDB exceptions.
 *
 * @author Artem Orobets enisher@gmail.com
 */
public class OrientPersistenceExceptionTranslator implements PersistenceExceptionTranslator {

    /**
     * Convert the given runtime exception to an appropriate exception from the
     * <code>org.springframework.dao</code> hierarchy.
     * Return null if no translation is appropriate: any other exception may
     * have resulted from user code, and should not be translated.
     *
     * @param ex - exception that must be translated
     * @return appropriate exception
     */
    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {

        if (ex instanceof IllegalStateException) {
            return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
        } else if (ex instanceof IllegalArgumentException) {
            return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);

        } else if (ex instanceof OConcurrentModificationException) {
            return new OptimisticLockingFailureException(ex.getMessage(), ex);
        } else if (ex instanceof ORecordNotFoundException) {
            return new DataRetrievalFailureException(ex.getMessage(), ex);
        } else if (ex instanceof OTransactionException) {
            return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);

        } else if (ex instanceof OException) {
            return new OrientSystemException(ex.getMessage(), ex);

        } else {
            return null;
        }
    }
}
