package ua.kiev.prog.case2;

import ua.kiev.prog.shared.Id;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDAO<T> {
    private final Connection conn;
    private final String table;

    public AbstractDAO(Connection conn, String table) {
        this.conn = conn;
        this.table = table;
    }

    public void createTable(Class<T> cls) {
        Field[] fields = cls.getDeclaredFields();
        Field id = getPrimaryKeyField(null, fields);

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(table)
                .append("(");

        sql.append(id.getName())
                .append(" ")
                .append(" INT AUTO_INCREMENT PRIMARY KEY,");

        for (Field f : fields) {
            if (f != id) {
                f.setAccessible(true);

                sql.append(f.getName()).append(" ");

                if (f.getType() == int.class) {
                    sql.append("INT,");
                } else if (f.getType() == String.class) {
                    sql.append("VARCHAR(100),");
                } else
                    throw new RuntimeException("Wrong type");
            }
        }

        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");

        try {
            try (Statement st = conn.createStatement()) {
                st.execute(sql.toString());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void add(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            StringBuilder names = new StringBuilder();
            StringBuilder values = new StringBuilder();

            // insert into t (name,age) values("..",..

            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);

                    names.append(f.getName()).append(',');
                    values.append('"').append(f.get(t)).append("\",");
                }
            }

            names.deleteCharAt(names.length() - 1); // last ','
            values.deleteCharAt(values.length() - 1);

            String sql = "INSERT INTO " + table + "(" + names.toString() +
                    ") VALUES(" + values.toString() + ")";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }

            // TODO: get ID
            // SELECT - X

            //HW-1
            try (Statement statement = conn.createStatement()) { //пытаемся подключиться к базе данных с помощью обьекта
                //интерфейса Statement
                try (ResultSet rs = statement.executeQuery("SELECT * FROM + table")) { //в обьект типа ResultSet пытаемся
                    //положить результат запроса, который мы выполняем с помощью метода executeQuery() через обьект класса Statement
                    //(SELECT относится к операторам для получения данных из таблицы)
                    while (rs.next()) { //пока есть строки для чтения из таблици, мы
                        int intId = rs.getInt(1); //обьявляем целочисленную переменную = методу rs.getInt(1),
                        //который вернет данные N-й(для нас под №1, как альтернативу можно использовать название колонки -"id") колонки
                        // текущей строки как тип int
                        id.setAccessible(true); //открываем доступ к изменению данных
                        id.set(t, intId); //устанавливаем для поля id значения

                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            StringBuilder sb = new StringBuilder();

            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);

                    sb.append(f.getName())
                            .append(" = ")
                            .append('"')
                            .append(f.get(t))
                            .append('"')
                            .append(',');
                }
            }

            sb.deleteCharAt(sb.length() - 1);

            // update t set name = "aaaa", age = "22" where id = 5
            String sql = "UPDATE " + table + " SET " + sb.toString() + " WHERE " +
                    id.getName() + " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void delete(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            String sql = "DELETE FROM " + table + " WHERE " + id.getName() +
                    " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    //HW-2
    public List<T> getAll(Class<T> cls, String... columnsNames) { //для метода вернутьВсех в качестве второго параметра добавляем
        //параметр переменной длины (varArgs - по-сути это массив) типа String - для данных, которые будут инициализировать
        // название столбцов таблици
        List<T> res = new ArrayList<>(); //обьявляен список res с интерфейсом List типом дженерик с поведением класса ArrayList

        try {
            try (Statement st = conn.createStatement()) { //пытаемся подключиться к базе данных с помощью обьекта
                //интерфейса Statement
                try (ResultSet rs = st.executeQuery("SELECT * FROM " + table)) { //в обьект типа ResultSet пытаемся
                    //положить результат запроса, который мы выполняем с помощью метода executeQuery() через обьект класса
                    // Statement (SELECT относится к операторам для получения данных из таблицы)
                    ResultSetMetaData md = rs.getMetaData(); //получаем обьект интерфейса ResultSetMetaData для получения
                    //информации о таблице от сервера SQL

                    while (rs.next()) { //пока есть что считывать
                        T t = cls.getDeclaredConstructor().newInstance(); //!!! //инициализируем дженерик создавая новый обьект
                        //на основании типа класса самого дженерика, который может вызвать любой заддекларированный конструктор,
                        // независимо от числа параметров

                        for (String columnFromVarArgs : columnsNames) { //проходимся по каждому элементу параметра переменной
                            //длины(varArgs - массив) - т.е. по каждому названию всех столбцов таблицы
                            for (int i = 1; i <= md.getColumnCount(); i++) { //через ResultSetMetaData получаем количество
                                // столбцов таблицы и для каждого столбца:
                                String columnNameFromMetaDate = md.getColumnName(i); //обьявляем переменную и кладем в нее
                                //название столбца полученное с помощью ResultSetMetaData
                                if (columnNameFromMetaDate.equals(columnFromVarArgs)) { //если названия столбцов совпадают, то
                                    Field field = cls.getDeclaredField(columnNameFromMetaDate); //создаем поле класса Field и
                                    //инициализируем его по типу дженерика,
                                    field.setAccessible(true); //открываем доступ,чтобы получить доступ к приватному полю класса

                                    field.set(t, rs.getObject(columnNameFromMetaDate)); //и устанавливаем значения для поля
                                }
                            }
                        }
                        res.add(t);
                    }
                }
            }

            return res;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Field getPrimaryKeyField(T t, Field[] fields) {
        Field result = null;

        for (Field f : fields) {
            if (f.isAnnotationPresent(Id.class)) {
                result = f;
                result.setAccessible(true);
                break;
            }
        }

        if (result == null)
            throw new RuntimeException("No Id field found");

        return result;
    }
}
