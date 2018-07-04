import requests
import datetime
import json
import os
import argparse
import glob
import time


def split_time_frame(from_datetime, to_datetime, delta):
	result = []
	time_frame_start = from_datetime
	time_frame_end = from_datetime + delta + datetime.timedelta(milliseconds=-1)
	while time_frame_end <= to_datetime:
		result += [(time_frame_start, time_frame_end)]
		time_frame_start += delta
		time_frame_end += delta
	return result

def date_to_string(date):
	return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class GerryCrawlRun(object):
	def __init__(self, name, url, start_date, end_date=datetime.datetime(2018, 6, 1)):
		self.name = name
		self.url = url
		self.start_date = start_date
		self.end_date = end_date

	def download_changes(self, from_date, to_date):
		url = '%s/changes/?q=after:{%s} AND before:{%s}&o=DETAILED_LABELS&o=MESSAGES&o=DETAILED_ACCOUNTS&o=REVIEWED&o=ALL_FILES&o=ALL_COMMITS&o=ALL_REVISIONS' % (self.url, date_to_string(from_date), date_to_string(to_date))
		if self.name != 'libreoffice':
			url += '&o=REVIEWER_UPDATES'
		try:
			response = requests.get(url)
			if response.status_code >= 200 and response.status_code<300:
				return json.loads(response.text[5:])
			else:
				print(response.status_code, 'for', url)
				return []
		except Exception as e:
			print(e)
			print(response.url)
			print(response.text)
			print(response.status_code)
			return []

	def create_data_structure(self, crawl_directory='./gerry_data/'): 
		self.crawl_directory = crawl_directory + self.name + '/'

		for time_frame in split_time_frame(self.start_date, self.end_date, datetime.timedelta(hours=1)):
			day_str = time_frame[0].strftime('%Y-%m-%d')
			os.makedirs(self.crawl_directory + day_str, exist_ok=True)

	def refined_crawl(self, from_datetime, to_datetime, i=0):
		def needs_smaller_time_frame(changes, i):
			if changes:
				return '_more_changes' in changes[-1] or len(changes) >= 500
			else:
				return False

		deltas = [datetime.timedelta(hours=24), datetime.timedelta(hours=12), datetime.timedelta(hours=1), datetime.timedelta(minutes=30), datetime.timedelta(minutes=1), datetime.timedelta(seconds=1)]

		changes = []
		if i < len(deltas):
			for time_frame in split_time_frame(from_datetime, to_datetime, deltas[i]):
				changes_ = self.download_changes(time_frame[0], time_frame[1])
				if needs_smaller_time_frame(changes_, i):
					changes += self.refined_crawl(time_frame[0], time_frame[1], i+1)
				else:
					changes += changes_
		else:
			print('Too many changes between', from_datetime, 'and', to_datetime)
		return changes

	def crawl(self):
		all_folders = glob.glob(self.crawl_directory + '*')
		l = len(all_folders)
		for index, folder in enumerate(sorted(all_folders)):
			if os.listdir(folder):
				print(folder.split('/')[-1], 'has been already crawled')
			else:
				start_time = datetime.datetime.now()
				from_datetime = datetime.datetime.strptime(folder.split('/')[-1], '%Y-%m-%d')
				to_datetime = from_datetime + datetime.timedelta(hours=24) 
				changes = self.refined_crawl(from_datetime, to_datetime)

				for change in changes:
					file_name = folder + '/' + change['id'] + '.json'
					with open(file_name, 'w') as json_file:
						json.dump(change, json_file)
				process_time = datetime.datetime.now() - start_time

				print('%s (%i changes) took %s (status: %.2f%% | %s left)' % (from_datetime.strftime('%Y-%m-%d'), len(changes), process_time, index/l*100.0, (l-index)*process_time))


if __name__ == "__main__":

	gerry_instances = {
		'openstack': GerryCrawlRun('openstack', 'https://review.openstack.org', datetime.datetime(2011, 7, 1)), 
		'chromium': GerryCrawlRun('chromium', 'https://chromium-review.googlesource.com', datetime.datetime(2011, 4, 1)), 
		'gerrit': GerryCrawlRun('gerrit', 'https://gerrit-review.googlesource.com', datetime.datetime(2008, 7, 1)),
		'android': GerryCrawlRun('android', 'https://android-review.googlesource.com', datetime.datetime(2008, 7, 1)),
		'golang': GerryCrawlRun('golang', 'https://go-review.googlesource.com', datetime.datetime(2014, 11, 1)),
		'libreoffice': GerryCrawlRun('libreoffice', 'https://gerrit.libreoffice.org', datetime.datetime(2012, 3, 1)),
		'eclipse': GerryCrawlRun('eclipse', 'https://git.eclipse.org/r', datetime.datetime(2009, 10, 1)),
		'wikimedia': GerryCrawlRun('wikimedia', 'https://gerrit.wikimedia.org/r', datetime.datetime(2011, 9, 1)),
		'onap': GerryCrawlRun('onap', 'https://gerrit.onap.org/r', datetime.datetime(2017, 1, 1))
	}

	parser = argparse.ArgumentParser("gerry")
	parser.add_argument('gerry_instance', choices=list(gerry_instances))
	parser.add_argument('--crawl_directory', dest='crawl_directory', default='./gerry_data/')
	args = parser.parse_args()

	gerry_instances[args.gerry_instance].create_data_structure(args.crawl_directory)
	gerry_instances[args.gerry_instance].crawl()
