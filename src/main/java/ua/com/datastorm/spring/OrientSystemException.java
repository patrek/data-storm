package ua.com.datastorm.spring;

import org.springframework.dao.UncategorizedDataAccessException;

/**
 * Orient-specific subclass of UncategorizedDataAccessException, for errors
 * that don't match any concrete <code>org.springframework.dao</code> exceptions.
 *
 * @author Artem Orobets enisher@gmail.com
 */
public class OrientSystemException extends UncategorizedDataAccessException {
    /**
     * Constructor for UncategorizedDataAccessException.
     *
     * @param msg   the detail message
     * @param cause the exception thrown by underlying data access API
     */
    public OrientSystemException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
