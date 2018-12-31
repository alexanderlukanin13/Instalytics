"""
The retrieve program retrieves JSONs and pictures from instagram
"""
import logging
import random
import re
import shutil
import time
import os

from datetime import datetime

import requests
import boto3

from botocore import errorfactory


def proxies_file():
    """Returns a list of proxies from a local file"""

    proxies = []
    with open('./config/agentstrings.conf', 'r') as file:
        useragent = file.read().splitlines()
    with open('./config/proxys.conf', 'r') as file:
        proxyfile = file.read().splitlines()
        for linenumber in range(len(proxyfile)):
            proxies.append((proxyfile[linenumber], random.choice(useragent)))

    return proxies

def grabjson(link, chosenproxy, useproxy=False):
    """
    grabjson grabs the JSON from Instagram
    :param link: Link that should be grabbed, e.g. 'https://www.instagram.com/president' for users
    :param chosenproxy: Proxy as string for requests module, e.g. '1.1.1.1:8080'
    :param useproxy: Indication if you want to use the proxy, default is False
    :return: The fetches JSON file as dict

    Todo: Better error handling in case the JSON can not be retrieved
    """
    log = logging.getLogger(__name__)

    if useproxy is True:
        proxy, useragent = chosenproxy

        try:
            jsonstarttime = time.time()
            resp = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy},
                                timeout=60)
            jsonendtime = time.time()

        except requests.exceptions.ProxyError:
            log.exception('Proxy not reachable')
            raise

        except requests.exceptions.ConnectTimeout:
            log.exception('Connection Timeout')
            raise

        except:
            log.exception('All other excptions')
            raise

    else:
        jsonstarttime = time.time()
        resp = requests.get(link, timeout=60)
        jsonendtime = time.time()

    if resp.status_code == 404:
        log.info('Page not found, 404: %s', link)
        resp.raise_for_status()

    if resp.status_code == 429:
        log.info('Too many requests for %s', link)
        resp.raise_for_status()

    resp_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', resp.text)
    jsontime = jsonendtime - jsonstarttime
    log.debug('%s: Time to retrieve (%s)', jsontime, link)

    return resp_json


def grabimage(file_directory,
              s3_link,
              s3_file_directory,
              pictureid,
              json,
              chosenproxy,
              useproxy=False):
    """
    grabimage grabs the image from th category and saves it where needed
    :param file_directory: The directory where the pictures should be safed
    :param s3_link: The connection to S3 from the main module
    :param s3_file_directory: The directory where the pictures should be safed
    :param pictureid: The picture ID
    :param json: The json retreived
    :param chosenproxy: The proxy that will be used
    :param useproxy: Should a proxe be used or not
    :return: None
    """

    imagelink = re.findall(r'"display_url":"([^"]+)"', json)
    filename = imagelink[0].split('/')[-1].split('?')[0]

    if useproxy is True:
        proxy, useragent = chosenproxy
        imagefile = requests.get(imagelink[0], headers={"User-Agent": useragent},
                                 proxies={"https": proxy}, timeout=60, stream=True)
    else:
        imagefile = requests.get(imagelink[0], timeout=60, stream=True)

    if not os.path.exists(file_directory):
        os.makedirs(file_directory)
    with open('{}/{}_{}'.format(file_directory, pictureid, filename), 'wb') as file:
        shutil.copyfileobj(imagefile.raw, file)

    s3_link.upload_file('{}/{}_{}'.format(file_directory, pictureid, filename), 'gvbinsta-test',
                        '{}/{}_{}'.format(s3_file_directory, pictureid, filename))


def writejson(file_directory,
              keyid,
              fetchedjson,
              s3_link,
              s3_directory):
    """
    Writing the JSON string where needed
    :param file_directory: Directory where the JSON string is saved
    :param keyid: Key ID for the file name
    :param fetchedjson: The fetched JSON
    :param s3_link: S3 connection
    :param s3_directory: S3 Directory where the files should be safed
    :return: None
    """
    log = logging.getLogger(__name__)
    if not os.path.exists(file_directory):
        os.makedirs(file_directory)
    try:
        with open('{}/{}.json'.format(file_directory, keyid), 'w') as file:
            file.write(datetime.now().strftime('%s') + '\n')
            file.write(fetchedjson[0])
    except:
        log.exception('%s could not be written, check for error. JSON is: %s', keyid, fetchedjson)
    s3_link.upload_file('{}/{}.json'.format(file_directory, keyid), 'gvbinsta-test',
                        '{}/{}.json'.format(s3_directory, keyid))

def set_retrieved_time(db_link, key, value):
    """
    Set the retrieved time within the DB for further processing
    :param db_link: DB connection
    :param key: Key that is used with the respective DB
    :param value: set retrieved value with the corresponding time
    :return: None
    """
    log = logging.getLogger(__name__)
    try:
        resp = db_link.update_item(
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

def set_deleted(db_link, key, value):
    """
    Set the item deleted within the DB
    :param db_link: DB connection
    :param key: Respective key for the category
    :param value: set deleted value as default
    :return: None
    """
    log = logging.getLogger(__name__)
    resp = db_link.update_item(
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
    Retrieve class extracts the JSON from Instagram, downloads the picture for posts,
    saves it locally and uploads it to Amazon S3 storage
    """
    def __init__(self,
                 useproxy=False,
                 awsprofile='default',
                 awsregion='eu-central-1',
                 storage_directory='./downloads'):

        self.log = logging.getLogger(__name__)
        self.proxies = proxies_file()
        self.useproxy = useproxy
        self.awssession = boto3.session.Session(profile_name=awsprofile, region_name=awsregion)
        self.s3_link = self.awssession.client('s3')
        self.dynamo = self.awssession.resource('dynamodb')
        self.picdb = self.dynamo.Table('te_post')
        self.locdb = self.dynamo.Table('te_location')
        self.userdb = self.dynamo.Table('te_user')
        self.storage_directory = storage_directory
        self.storage_json_location = 'json/location'
        self.storage_json_user = 'json/user'
        self.storage_json_post = 'json/post'
        self.storage_pictures = 'pictures'

    def retrieve_location(self, locationid):
        """
        Retrieve location details
        :param locationid: Location ID
        :return: None
        """
        fetchedjson = ''
        link = 'https://www.instagram.com/explore/locations/{}'.format(locationid)
        try:
            fetchedjson = grabjson(link,
                                   random.choice(self.proxies),
                                   self.useproxy)
        except requests.exceptions.HTTPError:
            self.log.exception('Http Error when retrieving the JSON')

        if fetchedjson not in ['', None]:
            self.log.debug('%s: Fetched JSON %s.', locationid, fetchedjson)
            file_storage_json_location = os.path.join(self.storage_directory,
                                                      self.storage_json_location)
            writejson(file_storage_json_location, locationid, fetchedjson, self.s3_link,
                      self.storage_json_location)
            set_retrieved_time(self.locdb, 'id', locationid)

        else:
            self.log.debug('Location %s: No JSON retrieved', locationid)
            set_deleted(self.locdb, 'id', locationid)


    def retrieve_user(self, userid):
        """
        Retrieve user details
        :param userid: User ID
        :return: None
        """
        link = 'https://www.instagram.com/{}/'.format(userid)
        fetchedjson = grabjson(link, random.choice(self.proxies), self.useproxy)

        if fetchedjson not in ['', None]:
            self.log.debug('%s: Fetched JSON %s.', userid, fetchedjson)
            file_storage_json_user = os.path.join(self.storage_directory,
                                                  self.storage_json_user)
            writejson(file_storage_json_user, userid, fetchedjson, self.s3_link,
                      self.storage_json_user)
            set_retrieved_time(self.userdb, 'username', userid)

        else:
            self.log.debug('Location %s: No JSON retrieved', userid)
            set_deleted(self.userdb, 'username', userid)

    def retrieve_picture(self, pictureid):
        """
        Retrieve picture details
        :param pictureid: Picture ID
        :return: None
        """
        link = 'https://www.instagram.com/p/{}/'.format(pictureid)
        fetchedjson = grabjson(link, random.choice(self.proxies), self.useproxy)

        if fetchedjson not in ['', None]:
            self.log.debug('%s: Fetched JSON %s', pictureid, fetchedjson)
            file_storage_json_post = os.path.join(self.storage_directory, self.storage_json_post)
            writejson(file_storage_json_post, pictureid, fetchedjson, self.s3_link,
                      self.storage_json_post)
            file_storage_pictures = os.path.join(self.storage_directory, self.storage_pictures)
            try:
                grabimage(file_storage_pictures,
                          self.s3_link,
                          self.storage_pictures,
                          pictureid,
                          fetchedjson[0],
                          random.choice(self.proxies),
                          self.useproxy)
            except:
                self.log.exception('Check the exception with the following: %s, %s',
                                   pictureid, fetchedjson)
            set_retrieved_time(self.picdb, 'shortcode', pictureid)

        else:
            self.log.debug('%s: No JSON for picture retrieved', pictureid)
            set_deleted(self.picdb, 'shortcode', pictureid)
