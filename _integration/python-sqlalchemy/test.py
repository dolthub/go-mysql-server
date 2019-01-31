import unittest
import pandas as pd
import sqlalchemy


class TestMySQL(unittest.TestCase):

    def test_connect(self):
        engine = sqlalchemy.create_engine('mysql+pymysql://root:@127.0.0.1:3306/mydb')
        with engine.connect() as conn:
            expected = {
                "name":  {0: 'John Doe', 1: 'John Doe', 2: 'Jane Doe', 3: 'Evil Bob'},
                "email": {0: 'john@doe.com', 1: 'johnalt@doe.com', 2: 'jane@doe.com', 3: 'evilbob@gmail.com'},
                "phone_numbers": {0: '["555-555-555"]', 1: '[]', 2: '[]', 3: '["555-666-555","666-666-666"]'},
            }

            repo_df = pd.read_sql_table("mytable", con=conn)
            d = repo_df.to_dict()
            del d["created_at"]
            self.assertEqual(expected, d)


if __name__ == '__main__':
    unittest.main()
