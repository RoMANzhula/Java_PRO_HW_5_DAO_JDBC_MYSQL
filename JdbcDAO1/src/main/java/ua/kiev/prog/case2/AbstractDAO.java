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
            try (Statement statement = conn.createStatement()) { // спробуємо підключитися до бази даних за допомогою об'єкта
                // інтерфейсу Statement
                try (ResultSet rs = statement.executeQuery("SELECT * FROM + table")) { // у об'єкт типу ResultSet намагаємося
                    // зберегти результат запиту, який ми виконуємо за допомогою методу executeQuery() через об'єкт класу Statement
                    // (SELECT відноситься до операторів для отримання даних з таблиці)
                    while (rs.next()) { // поки є рядки для читання з таблиці, ми
                    int intId = rs.getInt(1); // оголошуємо ціле числове значення для змінної, що містить результат методу rs.getInt(1),
                    // який повертає дані з N-го стовпця поточного рядка (для нас під №1, як альтернативу можна використовувати назву колонки -"id") як тип int
                    id.setAccessible(true); // відкриваємо доступ до зміни даних
                    id.set(t, intId); // встановлюємо для поля id значення
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
    public List<T> getAll(Class<T> cls, String... columnsNames) { //для методу getAll як другий параметр додаємо
        // параметр змінної довжини (varArgs - по суті це масив) типу String - для даних, які будуть ініціалізувати
        // назви стовпців таблиці
        List<T> res = new ArrayList<>(); //оголошуємо список res з інтерфейсом List і типом дженерик з поведінкою класу ArrayList

        try {
            try (Statement st = conn.createStatement()) { //намагаємось підключитися до бази даних через об'єкт
                // інтерфейсу Statement
                try (ResultSet rs = st.executeQuery("SELECT * FROM " + table)) { //в об'єкт типу ResultSet намагаємось
                    // помістити результат запиту, який виконується через метод executeQuery() за допомогою об'єкта класу
                    // Statement (SELECT — це оператор для отримання даних з таблиці)
                    ResultSetMetaData md = rs.getMetaData(); //отримуємо об'єкт інтерфейсу ResultSetMetaData для отримання
                    // інформації про таблицю від сервера SQL

                    while (rs.next()) { //поки є що зчитувати
                        T t = cls.getDeclaredConstructor().newInstance(); //!!! //ініціалізуємо дженерик, створюючи новий об'єкт
                        // на основі типу класу самого дженерика, який може викликати будь-який задекларований конструктор,
                        // незалежно від кількості параметрів

                        for (String columnFromVarArgs : columnsNames) { //проходимося по кожному елементу параметра змінної
                            // довжини (varArgs - масив) - тобто по кожному назві стовпців таблиці
                            for (int i = 1; i <= md.getColumnCount(); i++) { //через ResultSetMetaData отримуємо кількість
                                // стовпців таблиці і для кожного стовпця:
                                String columnNameFromMetaDate = md.getColumnName(i); //оголошуємо змінну і кладемо в неї
                                // назву стовпця, отриману через ResultSetMetaData
                                if (columnNameFromMetaDate.equals(columnFromVarArgs)) { //якщо назви стовпців співпадають, то
                                    Field field = cls.getDeclaredField(columnNameFromMetaDate); //створюємо поле класу Field і
                                    // ініціалізуємо його по типу дженерика,
                                    field.setAccessible(true); //відкриваємо доступ, щоб отримати доступ до приватного поля класу

                                    field.set(t, rs.getObject(columnNameFromMetaDate)); //і встановлюємо значення для поля
                                }
                            }
                        }
                        res.add(t); //додаємо об'єкт t до списку результатів
                    }
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
