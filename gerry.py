import requests
import datetime
import json
import os
import argparse
import glob
import logging
import tqdm
import time

log = logging.getLogger('gerry')


def config_logging(data_dir):
    global log
    log.setLevel(logging.DEBUG)
    log_name = os.path.join(data_dir, 'gerry-crawl.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    file_handler = logging.FileHandler(log_name)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    return log


def create_time_frames(from_datetime, to_datetime, frame_size):
    # [from_datetime, to_datetime[
    result = []
    time_frame_start = from_datetime
    time_frame_end = from_datetime + frame_size + \
        datetime.timedelta(milliseconds=-1)
    while time_frame_end <= to_datetime:
        result += [(time_frame_start, time_frame_end)]
        time_frame_start += frame_size
        time_frame_end += frame_size
    return result


def datetime_to_string(date):
    return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class Gerry(object):
    def __init__(self, name, url, start_date, end_date, directory='./gerry_data/'):
        self.name = name
        self.url = url
        self.directory = os.path.join(directory, name)
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

            if response and response.status_code >= 200 and response.status_code < 300:
                changes_subset = json.loads(response.text[5:])
            else:
                if response.status_code == 429:
                    time.sleep(10)
                raise HTTPError(response.status_code)

            if changes_subset:
                more_changes = '_more_changes' in changes_subset[-1]
                changes += changes_subset
            else:
                more_changes = False
            offset += len(changes_subset)
        return changes

    def get_change(self, change_number):
        url = '%s/changes/%s/detail/?o=DETAILED_LABELS&o=MESSAGES&o=DETAILED_ACCOUNTS&o=REVIEWED&o=ALL_FILES&o=ALL_COMMITS&o=ALL_REVISIONS' % (
                    self.url, change_number)
        if self.name != 'libreoffice':
            url += '&o=REVIEWER_UPDATES'

        response = requests.get(url)
        if response and response.status_code >= 200 and response.status_code < 300:
            change = json.loads(response.text[5:])
            file_name = str(change_number) + '.json'
            with open(os.path.join(folder, file_name), 'w') as json_file:
                json.dump(change, json_file)
        else:
            if response.status_code == 429:
                time.sleep(10)
            raise HTTPError(response.status_code)


    def run(self):
        for time_frame in create_time_frames(self.start_date, self.end_date, datetime.timedelta(hours=24)):
            day_str = time_frame[0].strftime('%Y-%m-%d')
            os.makedirs(os.path.join(self.directory,
                                     'changes', day_str), exist_ok=True)

        all_folders = glob.glob(os.path.join(self.directory, 'changes', '*'))

        complete = False

        while completed:
            complete = True # oh miss you, do...while loop
            empty_folders = [folder for folder in all_folders if os.listdir(folder)]

            for folder in tqdm.tqdm(empty_folders):
                change_numbers = []
                changes = []
                day_string = os.path.split(folder)[1]

                day = datetime.datetime.strptime(day_string, '%Y-%m-%d')
                try:
                    changes = self.get_changes(day)
                except Exception as exception:            
                    if isinstance(exception, ConnectionError):
                        log.error('GET changes for day %s failed' % (day))
                    elif isinstance(exception, json.JSONDecodeError):
                        log.error('Reading JSON for changes %s failed' % (day))
                    elif isinstance(exception, Exception): 
                        log.error('Unknown error occurred for day %s: %s' % (day, e))
                    complete = False
                                       
                change_numbers += [change['_number'] for change in changes]

                for change_number in change_numbers:
                    try:
                        self.get_change(change_number)
                    except Exception as exception:
                        if isinstance(exception, ConnectionError):
                            log.error('GET change %s failed' % (change_number))
                        elif isinstance(exception, json.JSONDecodeError):
                            log.error('Reading JSON for changes %s failed' % (change_number))
                        elif isinstance(exception, Exception):
                            log.error('Unknown error occurred for change %s: %s' % (change_number, exception))
                        complete = False


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

    gerry = Gerry(args.gerry_instance, data[args.gerry_instance]['url'],
                                 data[args.gerry_instance]['start_datetime'], datetime.datetime(2018, 7, 1), args.directory)
    config_logging(gerry.directory)

    gerry.run()
