import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
public class DatabaseConnection {

    public static Connection getConnection() throws SQLException, ClassNotFoundException {

        // Загрузка драйвера в JVM
        Class.forName("org.postgresql.Driver");
        // Объект для подключения к БД
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/NewFlywayDB",
                "postgres",
                "postgres"
        );
        return conn;
    }


}
