import datetime
import unittest
import gerry


class DatetimeToString(unittest.TestCase):

    def test_datetime_to_string(self):
        date = datetime.datetime(2018, 1, 1)
        self.assertEqual(gerry.datetime_to_string(
            date), '2018-01-01 00:00:00.000')

        date = datetime.datetime(2018, 1, 1, 20, 0, 0)
        self.assertEqual(gerry.datetime_to_string(
            date), '2018-01-01 20:00:00.000')


class CreateTimeFrames(unittest.TestCase):

    def test_create_time_frames_year(self):
        start_date = datetime.datetime(2017, 1, 1)
        end_date = datetime.datetime(2018, 1, 1)
        timeframes = gerry.create_time_frames(
            start_date, end_date, datetime.timedelta(hours=24))
        self.assertEqual(len(timeframes), 365)

    def test_create_time_frames_day(self):
        start_date = datetime.datetime(2017, 1, 1)
        end_date = datetime.datetime(2017, 1, 2)
        timeframes = gerry.create_time_frames(
            start_date, end_date, datetime.timedelta(hours=1))
        self.assertEqual(len(timeframes), 24)


if __name__ == '__main__':
    unittest.main()
