import unittest
import pymysql.cursors

class TestMySQL(unittest.TestCase):

    def test_connect(self):
        connection = pymysql.connect(host='127.0.0.1',
                                     user='root',
                                     password='',
                                     db='',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "SELECT name, email FROM mytable ORDER BY name, email"
                cursor.execute(sql)
                rows = cursor.fetchall()

                expected = [
                    {"name": "Evil Bob", "email": "evilbob@gmail.com"},
                    {"name": "Jane Doe", "email": "jane@doe.com"},
                    {"name": "John Doe", "email": "john@doe.com"},
                    {"name": "John Doe", "email": "johnalt@doe.com"}
                ]

                self.assertEqual(expected, rows)
        finally:
            connection.close()


if __name__ == '__main__':
    unittest.main()
