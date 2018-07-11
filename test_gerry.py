import datetime
import unittest
from unittest.mock import patch

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


class GerryCrawler(unittest.TestCase):

    @patch('os.makedirs')
    def setUp(self, mock_makedirs):
        mock_makedirs.return_value = True
        self.gerry = gerry.GerryCrawler('gerrit', 'https://gerrit-review.googlesource.com',
                                        datetime.datetime(2018, 6, 1), datetime.datetime(2018, 6, 2), './gerry_data/')

    def test_get_changes(self):
        changes = self.gerry.get_changes(datetime.datetime.strptime('2018-06-01', '%Y-%m-%d'))
        self.assertEqual(len(changes), 21)
        self.assertEqual(changes[0]['change_id'], 'Ib051dd347eaea2c77ae6c403ebf76bed4b9b4b9c')

    def test_get_changes_no_data(self):
        changes = self.gerry.get_changes(datetime.datetime.strptime('5018-06-01', '%Y-%m-%d'))
        self.assertEqual(len(changes), 0)


if __name__ == '__main__':
    unittest.main()
