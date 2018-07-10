import datetime
import unittest

class DatetimeToString(unittest.TestCase):

    def test_datetime_to_string(self):
    	date = datetime.datetime(2018, 1, 1)
        self.assertEqual(datetime_to_string(date), '2018-01-01')

        date = datetime.datetime(2018, 1, 1, 20, 0, 0)
        self.assertEqual(datetime_to_string(date), '2018-01-01')


if __name__ == '__main__':
    unittest.main()

