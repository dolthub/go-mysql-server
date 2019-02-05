package tech.sourced.jdbcmariadb;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class MySQLTest {

    @Test
    void test() {
        String dbUrl = "jdbc:mariadb://127.0.0.1:3306/mydb?user=root&password=";
        String query = "SELECT name, email FROM mytable ORDER BY name, email";
        List<Result> expected = new ArrayList<>();
        expected.add(new Result("Evil Bob", "evilbob@gmail.com"));
        expected.add(new Result("Jane Doe", "jane@doe.com"));
        expected.add(new Result("John Doe", "john@doe.com"));
        expected.add(new Result("John Doe", "johnalt@doe.com"));

        List<Result> result = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        result.add(new Result(rs.getString(1), rs.getString(2)));
                    }
                }
            }
        } catch (SQLException e) {
            fail(e);
        }

        assertEquals(expected, result);
    }

    class Result {
        String name;
        String email;

        Result(String name, String email) {
            this.name = name;
            this.email = email;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return Objects.equals(name, result.name) &&
                    Objects.equals(email, result.email);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "name='" + name + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }
    }
}
