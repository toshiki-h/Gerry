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

    def wait_for_server(status_code):
        # https://cloud.google.com/service-control/troubleshooting#how_do_i_perform_a_retry_on_api_errors
        GOOGLE_SERVER_WAITING_TIME = {429: 31, 500: 1, 503: 1}
        if status_code in GOOGLE_SERVER_WAITING_TIME:
            time.sleep(GOOGLE_SERVER_WAITING_TIME[status_code])

    def handle_exception(exception, change_type):
        if isinstance(exception, requests.exceptions.RequestException):
            if exception.response != None:
                log.error('GET %s failed with http status %i' % (
                    change_type, exception.response.status_code))
                Gerry.wait_for_server(
                    exception.response.status_code)
            else:
                log.error('GET %s %s failed with error: %s' % (change_type,
                                                               exception))
        elif isinstance(exception, json.JSONDecodeError):
            log.error(
                'Reading JSON for %s failed' % (change_type))
        elif isinstance(exception, Exception):
            log.error('Unknown error occurred for %s %s: %s' % (change_type,
                                                                exception))

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
            response.raise_for_status()

            changes_subset = json.loads(response.text[5:])
            if changes_subset:
                more_changes = '_more_changes' in changes_subset[-1]
                changes += changes_subset
            else:
                more_changes = False
            offset += len(changes_subset)
        return changes

    def get_change(self, change_number, to_folder):
        url = '%s/changes/%s/detail/?o=DETAILED_LABELS&o=MESSAGES&o=DETAILED_ACCOUNTS&o=REVIEWED&o=ALL_FILES&o=ALL_COMMITS&o=ALL_REVISIONS' % (
            self.url, change_number)
        if self.name != 'libreoffice':
            url += '&o=REVIEWER_UPDATES'

        response = requests.get(url)
        response.raise_for_status()

        change = json.loads(response.text[5:])
        file_name = str(change_number) + '.json'
        with open(os.path.join(folder, file_name), 'w') as json_file:
            json.dump(change, json_file)

    def run(self):
        for time_frame in create_time_frames(self.start_date, self.end_date, datetime.timedelta(hours=24)):
            day_str = time_frame[0].strftime('%Y-%m-%d')
            os.makedirs(os.path.join(self.directory,
                                     'changes', day_str), exist_ok=True)

        all_day_paths = glob.glob(os.path.join(self.directory, 'changes', '*'))

        complete = False

        while not complete:
            complete = True
            day_paths_pending = [
                day_path for day_path in all_day_paths if not os.listdir(day_path)]

            log.info(
                'Started new crawl iteration to crawl %i pending days' % (len(day_paths_pending)))

            for day_path in tqdm.tqdm(day_paths_pending):
                change_numbers = []
                changes = []

                day = datetime.datetime.strptime(
                    os.path.split(day_path)[1], '%Y-%m-%d')
                try:
                    changes = self.get_changes(day)
                except Exception as exception:
                    Gerry.handle_exception(exception, 'changes on %s' % (day))
                    complete = False

                change_numbers += [change['_number'] for change in changes]

                for change_number in change_numbers:
                    try:
                        self.get_change(change_number, day_path)
                    except Exception as exception:
                        Gerry.handle_exception(
                            exception, 'change %i' % (change_number))
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
