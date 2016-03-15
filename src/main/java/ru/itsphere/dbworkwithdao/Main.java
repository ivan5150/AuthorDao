package ru.itsphere.dbworkwithdao;

import ru.itsphere.dbworkwithdao.dao.AuthorDao;
import ru.itsphere.dbworkwithdao.dao.AuthorDaoJdbcImpl;
import ru.itsphere.dbworkwithdao.dao.ConnectionFactory;
import ru.itsphere.dbworkwithdao.domain.Author;

public class Main {

    private static AuthorDao authorDao = null;

    static {
        authorDao = new AuthorDaoJdbcImpl(ConnectionFactory.getInstance());
    }

    public static void main(String[] args) {
        authorDao.deleteAll();
        Author sasha = new Author(0, "Sasha", "World union");
        Author pasha = new Author(0, "Pasha", "World union");
        Author dasha = new Author(0, "Dasha", null);

        authorDao.insert(sasha);
        authorDao.insert(pasha);
        authorDao.insert(dasha);

        Author last = null;
        System.out.println("1 -------------------------");
        for (Author author : authorDao.getAll()) {
            System.out.println(author);
            last = author;
        }

        last.setTradeUnion("European union");
        System.out.println("2 -------------------------");
        authorDao.update(last);
        authorDao.getAll().forEach(System.out::println);

        System.out.println("3 -------------------------");
        System.out.println(authorDao.getById(last.getId()));

        System.out.println("4 -------------------------");
        authorDao.deleteById(last.getId());
        authorDao.getAll().forEach(System.out::println);

        System.out.println("5 -------------------------");
        authorDao.deleteAll();
        authorDao.getAll().forEach(System.out::println);
    }
}
