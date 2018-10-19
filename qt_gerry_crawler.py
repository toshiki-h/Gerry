
# coding: utf-8

# In[1]:


import pymongo, time, logging
import requests, json, sys
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import pprint


# In[2]:


db_name = 'qt_20180801'
base_url = 'https://codereview.qt-project.org/'
multiThread_cpu_num = 36
ACCEPTABLE_ERROR = 10
connection_error = 0
max_attempt = 3

client = pymongo.MongoClient()
db = client[db_name]
base_url = base_url
changes_collection = db['reviews']
comments_collection = db['comments']
inlines_collection = db['inlines']

def print_dict(d):
  new = {}
  for k, v in d.items():
    if isinstance(v, dict):
      v = print_dict(v)
    new[k.replace('.', '-')] = v
  return new

def replaceMongodbInvalidLetter(jsons):
    jsons_replaced = []
    for i in range(0, len(jsons)):
        json_replaced = {}
        for k in jsons[i]:
            json_replaced[k.replace('.', '_').replace('$', '')] = jsons[i][k]
        jsons_replaced.append(json_replaced)
    return jsons_replaced

###
def crawl_detail(reviewIdNum, latestPatchSetNum):
    global connection_error
    crawl_url_detail = base_url + "changes/%s/detail" % reviewIdNum
    all_det_inl = {'comments': [], 'inlines': []}
    for attempt_detail in range(max_attempt):
        try:
            r_detail = requests.get(crawl_url_detail)
            detail_raw = r_detail.text
            if detail_raw.startswith(")]}'"):
                detail_json = json.loads(detail_raw[4:])
                all_det_inl['comments'].append(detail_json)

                # Get the max number of patch set number
                rev_nums = int(latestPatchSetNum)

                # Get the inline comments for each revision of the change
                for rev_num in range(1, rev_nums+1):
#                     print('Latest: %s... rev_num=%s' % (rev_nums, rev_num))
                    crawl_url_inline = base_url + "changes/%s/revisions/%s/" % (reviewIdNum, rev_num)
                    for attempt_inline in range(max_attempt):
                        try:
                            r_inline = requests.get(crawl_url_inline)
                            inline_raw = r_inline.text
                            if inline_raw.startswith(")]}'"):
                                inline_json = json.loads(inline_raw[4:])
                                inline_json_replaced = {}
                                for fileKey in inline_json:
                                    inline_json_replaced[fileKey.replace('.', '_')] = inline_json[fileKey]
                                if inline_json != {}:
                                    inline_json_with_id = {}
                                    inline_json_with_id['_number'] = reviewIdNum
                                    inline_json_with_id['rev_num'] = rev_num
                                    inline_json_with_id['inline_comments'] = inline_json_replaced
                                    all_det_inl['inlines'].append(inline_json_with_id)
                            break
                        except requests.exceptions.RequestException as e:
                            if attempt_inline == max_attempt - 1:
                                logging.exception(e)
                                logging.exception('*** Exception occured while crawling %s ***' % crawl_url_inline)
            break
        except requests.exceptions.RequestException as e:
            if attempt_detail == max_attempt - 1:
                logging.exception(e)
                logging.exception('*** Exception occured while crawling %s ***' % crawl_url_detail)

    if all_det_inl['comments'] == []:
        logging.warning("*** no detail entry for %s ***" % crawl_url_detail)
    if all_det_inl['inlines'] == []:
        logging.debug("*** no inline comment for %s ***" % crawl_url_detail)

    return all_det_inl
###

def crawl_new_api(status, numRequests):
    count_changes = 0
    global connection_error
    change_json = None
    lastKey = None
    roundIdx= 0
    count = 0
    while change_json == None or '_more_changes' in change_json[-1]:
        roundIdx += 1
        try:
            if lastKey == None:
                # First round ---> no N parameter
                crawl_url_change = base_url + "changes/?q=status:%s&o=ALL_REVISIONS&o=ALL_FILES&o=ALL_COMMITS&o=MESSAGES&o=DETAILED_ACCOUNTS&n=%s"   % (status, numRequests)
            else:
                # From secod round  ---> use N parameter: the indicator of the starting review
                assert(lastKey != None)
                crawl_url_change = base_url + "changes/?q=status:%s&o=ALL_REVISIONS&o=ALL_FILES&o=ALL_COMMITS&o=MESSAGES&o=DETAILED_ACCOUNTS&n=%s&N=%s"   % (status, numRequests, lastKey)
            logging.info('*** Start crawling n=%s, _sortKey=%s (status: %s), %sth round ***' % (count_changes, lastKey, status, roundIdx))
            print('*** Start crawling n=%s, _sortKey=%s (status: %s), %sth round ***' % (count_changes, lastKey, status, roundIdx))
            r_change = requests.get(crawl_url_change)
            change_raw = r_change.text
        except requests.exceptions.RequestException as e:
            logging.exception(e)
            time.sleep(10)
            connection_error += 1
            if connection_error > ACCEPTABLE_ERROR:
                logging.exception("Too many errors when crawling changes (> %s), abort this script." % ACCEPTABLE_ERROR)
                logging.exception("Last crawled: %s" % crawl_url_change)
                sys.exit(1)
            else:
                continue

        if change_raw.startswith(")]}'"):
            change_json = json.loads(change_raw[4:])
            count_changes += len(change_json)
            if len(change_json) > 0:
                change_json_replaced = [print_dict(j) for j in change_json]
                changes_collection.insert_many(
                    replaceMongodbInvalidLetter(change_json_replaced))
            lastKey = change_json[-1]['_sortkey']
            #lastKey = None
            reviewIdNums = []
            latestPatchSetNums = []
            for each_change in change_json_replaced:
                reviewIdNums.append(each_change['_number'])
                if (len(each_change['revisions'].keys()) > 0):
                    assert(len(each_change['revisions'].keys()) == 1)
                    for commitId in each_change['revisions'].keys():
                        latestPatchSetNums.append(each_change['revisions'][commitId]['_number'])
                else: # no revision info
                    latestPatchSetNums.append(0)
                    print('No revision info: %s' % (each_change['_number']))
            if(len(reviewIdNums) != len(latestPatchSetNums)):
                print('%s %s' % (len(reviewIdNums), len(latestPatchSetNums)))
            assert(len(reviewIdNums) == len(latestPatchSetNums))
            # MULTITHREADING crawl_detail HERE
            pool = ThreadPool(multiThread_cpu_num)
            all_det_inl = []
            all_det_inl = pool.starmap(crawl_detail, zip(reviewIdNums, latestPatchSetNums))
            pool.close()
            pool.join()
            for det_inl in all_det_inl:
                if len(det_inl['comments']) != 0:
                    comments_collection.insert_many(
                        replaceMongodbInvalidLetter(det_inl['comments']))
                if len(det_inl['inlines']) != 0:
                    inlines_collection.insert_many(
                        replaceMongodbInvalidLetter(det_inl['inlines']))
        elif change_json == None or change_json == []:
            logging.exception("There is something wrong while crawling %s. Skip." % crawl_url_change)
            time.sleep(10)
            continue
        else:
            logging.exception("ERROR::: change_json is weird ::: connection_error = %s" % connection_error)
            time.sleep(10)
            continue


# In[3]:


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

  status = 'open'
  start_time = time.time()
  crawl_new_api(status, 500)
  logging.info('*** Crawling changes for "%s" status took %.2f sec ***' % (status, time.time()-start_time))

  status = 'abandoned'
  start_time = time.time()
  crawl_new_api(status, 500)
  logging.info('*** Crawling changes for "%s" status took %.2f sec ***' % (status, time.time()-start_time))

  status = 'merged'
  start_time = time.time()
  crawl_new_api(status, 500)
  logging.info('*** Crawling changes for "%s" status took %.2f sec ***' % (status, time.time()-start_time))
