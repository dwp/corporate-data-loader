import unittest
import corporate_data


class MyTestCase(unittest.TestCase):

    def test_works(self):
        (database, collection) = corporate_data.database_and_collection("db.database.collection")
        self.assertEqual("database", database)
        self.assertEqual("collection", collection)

    def test_dashes(self):
        (database, collection) = corporate_data.database_and_collection("db.database-1.collection-2")
        self.assertEqual("database-1", database)
        self.assertEqual("collection-2", collection)

    def test_rejects(self):
        self.assertRaises(ValueError, corporate_data.database_and_collection, "incorrect_format")


if __name__ == '__main__':
    unittest.main()
