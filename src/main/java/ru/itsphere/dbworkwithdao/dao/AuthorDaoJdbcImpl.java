package ru.itsphere.dbworkwithdao.dao;

import ru.itsphere.dbworkwithdao.domain.Author;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Реализация AuthorDao через JDBC.
 * <p>
 * http://it-channel.ru/
 *
 * @author 5150
 */
public class AuthorDaoJdbcImpl implements AuthorDao {

    public static final String SELECT_BY_ID_QUERY = "SELECT * FROM authors WHERE id = ?";
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_UNION = "trade_union";
    public static final String SELECT_ALL_QUERY = "SELECT * FROM authors";
    public static final String UPDATE_AUTHOR_QUERY = "UPDATE authors SET name = ?, trade_union = ? WHERE id = ?";//"UPDATE authors";
    public static final String DELETE_BY_ID_QUERY = "DELETE FROM authors WHERE id = ?";
    public static final String DELETE_ALL_AUTHORS = "DELETE FROM authors;";
    private static final String AUTHOR_ADD_VALUES = "INSERT INTO authors (name, trade_union) VALUES (?, ?);";
    private ConnectionFactory connectionFactory;

    public AuthorDaoJdbcImpl(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Author getById(long id) {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_ID_QUERY);) {
            statement.setLong(1, id);
            try (ResultSet resultSet = statement.executeQuery();) {
                while (resultSet.next()) {
                    return new Author(resultSet.getLong(COLUMN_ID),
                            resultSet.getString(COLUMN_NAME),
                            resultSet.getString(COLUMN_UNION));
                }
            }
        } catch (Exception e) {
            throw new DaoException(String.format("Method getById(id: '%d') has thrown an exception.", id), e);
        }
        return null;
    }

    @Override
    public void insert(Author author) {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(AUTHOR_ADD_VALUES);) {
            statement.setString(1, author.getName());
            statement.setString(2, author.getTradeUnion());
            int rowCount = statement.executeUpdate();
            if (rowCount == 0) {
                throw new RuntimeException("INSERT ERROR");
            }
        } catch (Exception e) {
            //e.printStackTrace();
            throw new DaoException(String.format("Method insert(Author author) has thrown an exception."), e);
        }
    }

    @Override
    public List<Author> getAll() {
        List<Author> all = new ArrayList<>();
        try (Connection connection = connectionFactory.getConnection();
             Statement statement = connection.createStatement();) {
            try (ResultSet resultSet = statement.executeQuery(SELECT_ALL_QUERY);) {
                while (resultSet.next()) {
                    all.add(new Author(resultSet.getLong(COLUMN_ID),
                            resultSet.getString(COLUMN_NAME),
                            resultSet.getString(COLUMN_UNION)));
                }
            }
        } catch (Exception e) {
            throw new DaoException("Method getAll() has thrown an exception.", e);
        }
        return all;
    }

    @Override
    public void update(Author author) {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE_AUTHOR_QUERY);) {
            statement.setString(1, author.getName());
            statement.setString(2, author.getTradeUnion());
            statement.setLong(3, author.getId());
            statement.executeUpdate();
        } catch (Exception e) {
            throw new DaoException(String.format("Method update(author:'%s') has thrown an exception", author), e);
        }
    }

    @Override
    public void deleteById(long id) {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_BY_ID_QUERY);) {
            statement.setLong(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            //e.printStackTrace();
            throw new DaoException(String.format("Method deleteById(long id) has thrown an exception."), e);
        }
    }

    @Override
    public void deleteAll() {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_ALL_AUTHORS);) {
            statement.executeUpdate();
        } catch (SQLException e) {
            // e.printStackTrace();
            throw new DaoException(String.format("Method deleteAll() has thrown an exception."), e);
        }
    }
}
