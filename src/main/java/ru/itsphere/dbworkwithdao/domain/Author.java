package ru.itsphere.dbworkwithdao.domain;

/**
 * Класс автор.
 * <p>
 * http://it-channel.ru/
 *
 * @author Budnikov Aleksandr
 */
public class Author {
    /**
     * Уникальный идетификатор авторов.
     */
    private long id;

    /**
     * Имя автора.
     */
    private String name;

    /**
     * Название союза к которому он принадлежит
     */
    private String tradeUnion;

    public Author(long id, String name, String tradeUnion) {
        this.id = id;
        this.name = name;
        this.tradeUnion = tradeUnion;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTradeUnion() {
        return tradeUnion;
    }

    public void setTradeUnion(String tradeUnion) {
        this.tradeUnion = tradeUnion;
    }

    @Override
    public String toString() {
        return "Author{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", tradeUnion='" + tradeUnion + '\'' +
                '}';
    }
}
