package ord.example;

import com.zaxxer.hikari.HikariDataSource;
import org.example.DataSourceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.System.out;

public class PgCopyTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:14"
    );

    HikariDataSource dataSource;

    @BeforeAll
    static void beforeAll() throws SQLException {
        postgres.start();
        HikariDataSource hikariDataSource = DataSourceFactory.create(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        createTableAndInitializeData(hikariDataSource);
        hikariDataSource.close();
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @BeforeEach
    void setUp() {
        dataSource = DataSourceFactory.create(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void clean() {
        dataSource.close();
    }

    @ParameterizedTest
    @CsvSource({
            // error | evict
            "false, false",
            "true, false",
            "true, true"})
    public void runTest(String error, String evict) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            final PGConnection pgConnection = connection.unwrap(PGConnection.class);
            try (PGCopyInputStream inputStream = new PGCopyInputStream(pgConnection, "COPY (SELECT * FROM example) TO STDOUT")) {
                if (error.equals("true")) {
                    throw new IllegalStateException();
                }
                printInputStream(inputStream);
            } catch (RuntimeException e) {
                if (evict.equals("true")) {
                    dataSource.evictConnection(connection);
                }
            }
        }

        System.out.println("Excuting select:");

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * from example where id=1");
            resultSet.next();
            System.out.println(resultSet.getString("value"));
        }
    }

    private static void printInputStream(PGCopyInputStream inputStream) throws IOException {
        int size = 0;
        byte[] buffer = new byte[1024];
        while ((size = inputStream.read(buffer)) != -1) out.write(buffer, 0, size);
    }

    static void createTableAndInitializeData(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO example (id, value) VALUES (?, ?)");) {
            String sql = """
                    CREATE TABLE example
                    (id INTEGER not NULL, 
                    value VARCHAR(255),
                    PRIMARY KEY ( id ))""";
            stmt.executeUpdate(sql);
            for (int i = 0; i < 10; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "value" + i);
                preparedStatement.executeUpdate();
            }
        }
    }
}
