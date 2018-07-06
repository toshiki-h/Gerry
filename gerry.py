import requests
import datetime
import json
import os
import argparse
import glob
import logging
import tqdm


class HTTPExecption(Exception):
    def __init__(self, status_code):
        super().__init__('HTTPExecption')
        self.status_code = status_code

def split_time_frame(from_datetime, to_datetime, delta):
	result = []
	time_frame_start = from_datetime
	time_frame_end = from_datetime + delta + datetime.timedelta(milliseconds=-1)
	while time_frame_end <= to_datetime:
		result += [(time_frame_start, time_frame_end)]
		time_frame_start += delta
		time_frame_end += delta
	return result

def datetime_to_string(date):
	return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

class GerryCrawlRun(object):
	def __init__(self, name, url, start_date, end_date, crawl_directory='./gerry_data/'):
		self.name = name
		self.url = url
		self.crawl_directory = crawl_directory + self.name + '/'
		self.start_date = start_date
		self.end_date = end_date

	def get_changes(self, from_datetime, to_datetime, offset):
		url = '%s/changes/?q=after:{%s} AND before:{%s} AND is:closed&S=%i' % (self.url, datetime_to_string(from_datetime), datetime_to_string(to_datetime), offset)
		response = requests.get(url)
		response = requests.get(url)
		if response.status_code >= 200 and response.status_code < 300:
			return json.loads(response.text[5:])
		else:
			raise HTTPExecption(response.status_code)

	def get_change_details(self, change_number):
		url = '%s/changes/%s/detail/?o=DETAILED_LABELS&o=MESSAGES&o=DETAILED_ACCOUNTS&o=REVIEWED&o=ALL_FILES&o=ALL_COMMITS&o=ALL_REVISIONS' % (self.url, change_number)

		if self.name != 'libreoffice':
			url += '&o=REVIEWER_UPDATES'
		response = requests.get(url)
		if response.status_code >= 200 and response.status_code < 300:
			return json.loads(response.text[5:])
		else:
			raise HTTPExecption(response.status_code)
		
	def get_changes_on_day(self, day):
		from_datetime = day
		to_datetime = from_datetime + datetime.timedelta(hours=24) + datetime.timedelta(milliseconds=-1)
		more_changes = True
		changes = []
		offset = 0

		while more_changes:
			changes_on_date = []
			try:
				changes_subset = self.get_changes(from_datetime, to_datetime, offset)
			except ValueError:
				log.error('Reading JSON for changes between %s and %s (offset %i) failed' % (from_datetime, to_datetime, offset) )
			except HTTPExecption as e:
				log.error('Crawling JSON for changes between %s and %s (offset %i) failed with HTTP status %i' % (from_datetime, to_datetime, offset, e.status_code) )
			
			if changes_subset:
				more_changes = '_more_changes' in changes_subset[-1]
				changes += changes_subset
			else:
				more_changes = False
			offset += len(changes_subset)
		return changes

	def crawl(self, end_date):
		for time_frame in split_time_frame(self.start_date, end_date, datetime.timedelta(hours=24)):
			day_str = time_frame[0].strftime('%Y-%m-%d')
			os.makedirs(self.crawl_directory + day_str, exist_ok=True)

		all_folders = glob.glob(self.crawl_directory + '*')
		l = len(all_folders)
		log.info(str(l) + ' days to crawl')

		change_numbers = []

		for index, folder in enumerate(tqdm.tqdm(sorted(all_folders))):
			change_numbers = []
			if os.listdir(folder):
				log.info(folder.split('/')[-1] + ' has been already crawled')
			else:
				day = datetime.datetime.strptime(folder.split('/')[-1], '%Y-%m-%d')
				changes = []
				try:
					changes = self.get_changes_on_day(day)
				except ValueError:
					log.error('Reading JSON for changes between %s and %s (offset %i) failed' % (from_datetime, to_datetime, offset) )
				except HTTPExecption as e:
					log.error('Crawling JSON for changes between %s and %s (offset %i) failed with HTTP status %i' % (from_datetime, to_datetime, offset, e.status_code) )

				change_numbers += [change['_number'] for change in changes]

			l = len(change_numbers)

			for index, change_number in enumerate(change_numbers):
				change = self.get_change_details(change_number)
				file_name = folder + '/' + str(change_number) + '.json'
				with open(file_name, 'w') as json_file:
					json.dump(change, json_file)


if __name__ == '__main__':

	data = {
		'openstack': {'url': 'https://review.openstack.org', 'start_datetime': datetime.datetime(2011, 7, 1)},
		'chromium': {'url': 'https://chromium-review.googlesource.com', 'start_datetime': datetime.datetime(2011, 4, 1)},
		'gerrit': {'url': 'https://gerrit-review.googlesource.com', 'start_datetime': datetime.datetime(2008, 7, 1)},
		'android': {'url': 'https://android-review.googlesource.com', 'start_datetime': datetime.datetime(2008, 7, 1)},
		'golang': {'url': 'https://go-review.googlesource.com', 'start_datetime': datetime.datetime(2014, 11, 1)},
		'libreoffice': {'url': 'https://gerrit.libreoffice.org', 'start_datetime': datetime.datetime(2012, 3, 1)},
		'eclipse': {'url': 'https://git.eclipse.org/r', 'start_datetime': datetime.datetime(2009, 10, 1)},
		'wikimedia': {'url': 'https://gerrit.wikimedia.org/r', 'start_datetime': datetime.datetime(2011, 9, 1)},
		'onap': {'url': 'https://gerrit.onap.org/r', 'start_datetime': datetime.datetime(2017, 1, 1)},
	}	

	parser = argparse.ArgumentParser('gerry')
	parser.add_argument('gerry_instance', choices=list(data))
	parser.add_argument('--crawl_directory', dest='crawl_directory', default='./gerry_data/')
	args = parser.parse_args()

	os.makedirs(args.crawl_directory, exist_ok=True)

	log = logging.getLogger('gerry')
	log.setLevel(logging.DEBUG)

	log_name = args.crawl_directory + 'crawl-' + str(args.gerry_instance) + '.log'
	formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')

	file_handler = logging.FileHandler(log_name)
	file_handler.setFormatter(formatter)
	log.addHandler(file_handler)

	# stream_handler = logging.StreamHandler()
	# stream_handler.setFormatter(formatter)
	# log.addHandler(stream_handler)

	gerry_instance = GerryCrawlRun(args.gerry_instance, data[args.gerry_instance]['url'], data[args.gerry_instance]['start_datetime'], args.crawl_directory)
	gerry_instance.crawl(datetime.datetime(2018, 7, 1))

