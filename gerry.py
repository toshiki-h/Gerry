import requests
import datetime
import json
import os
import argparse
import glob
import logging
import tqdm

log = logging.getLogger('gerry')


def config_logging(data_dir):
    global log
    log.setLevel(logging.DEBUG)
    log_name = data_dir + 'gerry-crawl.log'
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    file_handler = logging.FileHandler(log_name)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    return log


def split_time_frame(from_datetime, to_datetime, delta):
    result = []
    time_frame_start = from_datetime
    time_frame_end = from_datetime + delta + \
        datetime.timedelta(milliseconds=-1)
    while time_frame_end <= to_datetime:
        result += [(time_frame_start, time_frame_end)]
        time_frame_start += delta
        time_frame_end += delta
    return result


def datetime_to_string(date):
    return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class GerryCrawler(object):
    def __init__(self, name, url, start_date, end_date, directory='./gerry_data/'):
        self.name = name
        self.url = url
        self.directory = directory + self.name + '/'
        self.start_date = start_date
        self.end_date = end_date
        os.makedirs(self.directory, exist_ok=True)

    def get_changes(self, day):
        from_datetime = day
        to_datetime = from_datetime + \
            datetime.timedelta(hours=24) + datetime.timedelta(milliseconds=-1)
        more_changes = True
        changes = []
        offset = 0

        while more_changes:
            changes_subset = []
            url = '%s/changes/?q=after:{%s} AND before:{%s} AND is:closed&S=%i' % (
                self.url, datetime_to_string(from_datetime), datetime_to_string(to_datetime), offset)
            response = requests.get(url)

            if response.status_code >= 200 and response.status_code < 300:
                try:
                    changes_subset = json.loads(response.text[5:])
                except json.JSONDecodeError:
                    log.error('Reading JSON for changes between %s and %s (offset %i) failed' % (
                        from_datetime, to_datetime, offset))
            else:
                log.error('GET changes between %s and %s (offset %i) failed with HTTP status %i' % (
                    from_datetime, to_datetime, offset, response.status_code))

            if changes_subset:
                more_changes = '_more_changes' in changes_subset[-1]
                changes += changes_subset
            else:
                more_changes = False
            offset += len(changes_subset)
        return changes

    def run(self):
        for time_frame in split_time_frame(self.start_date, self.end_date, datetime.timedelta(hours=24)):
            day_str = time_frame[0].strftime('%Y-%m-%d')
            os.makedirs(self.directory + '/changes/' + day_str, exist_ok=True)

        all_folders = glob.glob(self.directory + '/changes/*')
        l = len(all_folders)
        log.info(str(l) + ' days to crawl')

        for index, folder in enumerate(tqdm.tqdm(sorted(all_folders))):
            change_numbers = []
            if os.listdir(folder):
                log.info(folder.split('/')[-1] + ' has been already crawled')
            else:
                day = datetime.datetime.strptime(
                    folder.split('/')[-1], '%Y-%m-%d')
                changes = self.get_changes(day)

                change_numbers += [change['_number'] for change in changes]

            for index, change_number in enumerate(change_numbers):
                url = '%s/changes/%s/detail/?o=DETAILED_LABELS&o=MESSAGES&o=DETAILED_ACCOUNTS&o=REVIEWED&o=ALL_FILES&o=ALL_COMMITS&o=ALL_REVISIONS' % (
                    self.url, change_number)

                if self.name != 'libreoffice':
                    url += '&o=REVIEWER_UPDATES'

                response = requests.get(url)
                if response.status_code >= 200 and response.status_code < 300:
                    try:
                        change = json.loads(response.text[5:])
                        file_name = folder + str(change_number) + '.json'
                        with open(file_name, 'w') as json_file:
                            json.dump(change, json_file)
                    except json.JSONDecodeError:
                        log.error('Reading JSON for change %i failed' %
                                  (change_number))
                else:
                    log.error('GET change %i failed with HTTP status %i' %
                              (change_number, response.status_code))


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
    parser.add_argument('--directory', dest='directory',
                        default='./gerry_data/')
    args = parser.parse_args()

    print(args.directory)

    os.makedirs(args.directory, exist_ok=True)

    gerry_crawler = GerryCrawler(args.gerry_instance, data[args.gerry_instance]['url'],
                                 data[args.gerry_instance]['start_datetime'], datetime.datetime(2018, 7, 1), args.directory)
    config_logging(gerry_crawler.directory)

    gerry_crawler.run()