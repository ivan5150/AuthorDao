package ru.itsphere.dbworkwithdao.dao;

import ru.itsphere.dbworkwithdao.domain.Author;

import java.util.List;

/**
 * Содержит методы необходимые для работы с объектами класса Author.
 * <p>
 * http://it-channel.ru/
 *
 * @author Budnikov Aleksandr
 */
public interface AuthorDao {

    /**
     * Возвращает объект класса Author с данными извлеченными из хранилища.
     * Для поиска используется идентификатор.
     *
     * @param id - соответствует колонке идентификаторов в БД.
     * @return объект класса Author с данными.
     */
    Author getById(long id);

    /**
     * Сохраняет объект класса Author в базу данных.
     *
     * @param author - объект класса Author с данными.
     */
    void insert(Author author);

    /**
     * Возвращает список всех аторов.
     *
     * @return список всех авторов.
     */
    List<Author> getAll();

    /**
     * Обновляет новую информацию об авторе.
     *
     * @param author - объект класса Author с данными.
     */
    void update(Author author);

    /**
     * Удаляет пользователя из базы данных по идентификатору.
     *
     * @param id - соответствует колонке идентификаторов в БД.
     */
    void deleteById(long id);

    /**
     * Удаляет всех авторов.
     */
    void deleteAll();
}
