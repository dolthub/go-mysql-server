#  Copyright 2020-2021 Dolthub, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
