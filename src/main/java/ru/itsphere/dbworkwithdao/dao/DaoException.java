package ru.itsphere.dbworkwithdao.dao;

/**
 * Исключение дао слоя
 * <p>
 * http://it-channel.ru/
 *
 * @author Budnikov Aleksandr
 */
public class DaoException extends RuntimeException {
    /**
     * Исключение дао слоя
     *
     * @param message текст описывающий условия при которых была ошибка
     * @param cause   причина (исключение)
     */
    public DaoException(String message, Throwable cause) {
        super(message, cause);
    }
}
