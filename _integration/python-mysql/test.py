import unittest
import mysql.connector

class TestMySQL(unittest.TestCase):

    def test_connect(self):
        connection = mysql.connector.connect(host='127.0.0.1',
                                     user='root',
                                     passwd='')

        try:
            cursor = connection.cursor()
            sql = "SELECT name, email FROM mytable ORDER BY name, email"
            cursor.execute(sql)
            rows = cursor.fetchall()

            expected = [
                ("Evil Bob", "evilbob@gmail.com"),
                ("Jane Doe", "jane@doe.com"),
                ("John Doe", "john@doe.com"),
                ("John Doe", "johnalt@doe.com")
            ]

            self.assertEqual(expected, rows)
        finally:
            connection.close()


if __name__ == '__main__':
    unittest.main()
