import pytest
import datetime

def test_datetime_to_string():
	date = datetime.datetime(2018, 1, 1)
	assert(datetime_to_string(date), '2018-01-01')

	date = datetime.datetime(2018, 1, 1, 20, 0, 0)
	assert(datetime_to_string(date), '2018-01-01')