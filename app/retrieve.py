import logging
import random
import requests
import re
import shutil
import boto3
import time
import os

from datetime import datetime
from botocore import errorfactory


def proxies_file():

    """Returns a list of proxies from a local file"""

    proxies = []

    with open('./config/agentstrings.conf', 'r') as f:
        useragent = f.read().splitlines()

    with open('./config/proxys.conf', 'r') as f:
        proxyfile = f.read().splitlines()
        for linenumber in range(len(proxyfile)):
            proxies.append((proxyfile[linenumber], random.choice(useragent)))

    return proxies

def grabjson(link, chosenproxy, useproxy=False):

    log = logging.getLogger(__name__)

    if useproxy == True:

        proxy, useragent = chosenproxy

        try:
            jsonstarttime = time.time()
            resp = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy}, timeout=60)
            # resp = requests.get(link, headers={"User-Agent": useragent, "Connection": "close"},
            #                     proxies={"https": proxy}, timeout=60)
            jsonendtime = time.time()

        except requests.exceptions.ProxyError as err:
            log.error('Proxy not reachable')
            log.error(err)
            raise SystemExit

    else:
        jsonstarttime = time.time()
        resp = requests.get(link, timeout=60)
        jsonendtime = time.time()

    if resp.status_code == 404:

        log.info('Page not found, 404: {}'.format(link))

        return None

    resp_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', resp.text)
    jsontime = jsonendtime - jsonstarttime
    log.debug('%s: Time to retrieve (%s)', jsontime, link)

    return resp_json


def grabimage(file_directory,
              s3,
              pictureid,
              json,
              chosenproxy,
              useproxy=False):

    imagelink = re.findall(r'"display_url":"([^"]+)"', json)

    filename = imagelink[0].split('/')[-1].split('?')[0]

    if useproxy == True:

        proxy, useragent = chosenproxy

        imagefile = requests.get(imagelink[0], headers={"User-Agent": useragent}, proxies={"https": proxy}, timeout=60,
                                 stream=True)

    else:

        imagefile = requests.get(imagelink[0], timeout=60, stream=True)

    if not os.path.exists(file_directory):
        os.makedirs(file_directory)
    with open('{}/{}_{}'.format(file_directory, pictureid, filename), 'wb') as f:
        shutil.copyfileobj(imagefile.raw, f)

    s3.upload_file('{}/{}_{}'.format(file_directory, pictureid, filename), 'gvbinsta-test',
                   'pictures/{}_{}'.format(pictureid, filename))


def writejson(file_directory,
              id,
              fetchedjson,
              s3):
    log = logging.getLogger(__name__)

    if not os.path.exists(file_directory):
        os.makedirs(file_directory)
    try:
        with open('{}/{}.json'.format(file_directory, id), 'w') as f:
            f.write(datetime.now().strftime('%s') + '\n')
            f.write(fetchedjson[0])
    except:
        log.exception('%s could not be written, check for error. JSON is: %s', id, fetchedjson)

    s3.upload_file('./downloads/json/{}.json'.format(id), 'gvbinsta-test',
                        'json/{}.json'.format(id))

def set_retrieved_time(db, key, value):

    log = logging.getLogger(__name__)

    try:

        resp = db.update_item(
            Key={
                key: value
            },
            UpdateExpression='SET retrieved_at_time = :rtime',
            ExpressionAttributeValues={
                ':rtime': int(time.time())
            }
        )

        log.debug(resp)

    except errorfactory.ClientError:
        log.exception('Exception when updating "retrieved_at_time" in DB')
        raise SystemExit

    log.debug(resp)

def set_deleted(db, key, value):
    log = logging.getLogger(__name__)

    resp = db.update_item(
        Key={
            key: value
        },
        UpdateExpression='SET deleted = :del, retrieved_at_time = :time',
        ExpressionAttributeValues={
            ':del': True,
            ':time': int(time.time())
        }
    )

    log.debug(resp)

class Retrieve():

    """
    Retrieve class extracts the JSON from Instagram, downloads the picture for posts, saves it locally and uploads
    it to Amazon S3 storage
    """

    def __init__(self,
                 useproxy=False,
                 awsprofile='default',
                 awsregion='eu-central-1',
                 storage_directory ='.'):

        self.log = logging.getLogger(__name__)
        self.proxies = proxies_file()
        self.useproxy = useproxy
        self.awssession = boto3.session.Session(profile_name=awsprofile, region_name=awsregion)
        self.s3 = self.awssession.client('s3')
        self.dynamo = self.awssession.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.userdb = self.dynamo.Table('test4')
        self.storage_directory = storage_directory
        self.storage_json_location = 'downloads/json/location'
        self.storage_json_user = 'downloads/json/user'
        self.storage_json_post = 'downloads/json/post'
        self.storage_pictures = 'downloads/pictures'

    def retrieve_location(self, locationid):

        link = 'https://www.instagram.com/explore/locations/{}'.format(locationid)

        fetchedjson = grabjson(link,
                               random.choice(self.proxies),
                               self.useproxy)

        if fetchedjson not in ['', None]:

            self.log.debug('{}: Fetched JSON {}.'.format(locationid,fetchedjson))
            file_storage_json_location = os.path.join(self.storage_directory, self.storage_json_location)
            writejson(file_storage_json_location, locationid, fetchedjson, self.s3)

            set_retrieved_time(self.locdb, 'id', locationid)

        else:
            self.log.debug('Location {}: No JSON retrieved'.format(locationid))

            set_deleted(self.locdb, 'id', locationid)


    def retrieve_user(self, userid):

        link = 'https://www.instagram.com/{}/'.format(userid)

        fetchedjson = grabjson(link, random.choice(self.proxies), self.useproxy)

        if fetchedjson not in ['', None]:

            self.log.debug('{}: Fetched JSON {}.'.format(userid, fetchedjson))

            file_storage_json_user = os.path.join(self.storage_directory, self.storage_json_user)
            writejson(file_storage_json_user, userid, fetchedjson, self.s3)

            set_retrieved_time(self.userdb, 'username', userid)

        else:

            self.log.debug('Location {}: No JSON retrieved'.format(userid))

            set_deleted(self.userdb, 'username', userid)


    def retrieve_picture(self, pictureid):

        link = 'https://www.instagram.com/p/{}/'.format(pictureid)

        fetchedjson = grabjson(link, random.choice(self.proxies), self.useproxy)

        if fetchedjson not in ['', None]:

            self.log.debug('{}: Fetched JSON {}.'.format(pictureid, fetchedjson))

            file_storage_json_post = os.path.join(self.storage_directory, self.storage_json_post)
            writejson(file_storage_json_post, pictureid, fetchedjson, self.s3)

            file_storage_pictures = os.path.join(self.storage_directory, self.storage_pictures)
            try:
                grabimage(file_storage_pictures,
                          self.s3,
                          pictureid,
                          fetchedjson[0],
                          random.choice(self.proxies),
                          self.useproxy)
            except:
                self.log.exception('Check the exception with the following: %s, %s, %s',
                                   pictureid, fetchedjson)

            set_retrieved_time(self.picdb, 'shortcode', pictureid)

        else:

            self.log.debug('%s: No JSON for picture retrieved', pictureid)

            set_deleted(self.picdb, 'shortcode', pictureid)