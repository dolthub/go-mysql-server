require "minitest/autorun"
require "mysql"

class TestMySQL < Minitest::Test
    def test_can_connect
        conn = Mysql::new("127.0.0.1", "root", "")
        res = conn.query "SELECT name, email FROM mytable ORDER BY name, email"

        expected = [
            ["Evil Bob", "evilbob@gmail.com"],
            ["Jane Doe", "jane@doe.com"],
            ["John Doe", "john@doe.com"],
            ["John Doe", "johnalt@doe.com"]
        ]

        rows = res.map do |row| [row[0], row[1]] end
        assert_equal rows, expected

        conn.close()
    end
end
