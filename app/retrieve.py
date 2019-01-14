"""
The retrieve program retrieves JSONs and pictures from Instagram.
"""
from datetime import datetime
import logging
import os
import random
import re
import shutil
import time

import boto3
from botocore import errorfactory
import requests

from .utils import read_lines, measure_time


def proxies_file():
    """
    Returns a list of tuples (proxy_ip, user_agent) from a local file.
    """
    user_agents = read_lines('./config/agentstrings.conf')
    proxies = read_lines('./config/proxys.conf')
    return [(x, random.choice(user_agents)) for x in proxies]


def get_json_instagram(link,
                       proxylist,
                       useproxy=False):
    """
    grabjson grabs the JSON from Instagram
    :param link: Link that should be grabbed, e.g. 'https://www.instagram.com/president' for users
    :param proxylist: Proxy list for requests module, e.g. ['1.1.1.1:8080', ...]
    :param key: Original key that created the link
    :param useproxy: Indication if you want to use the proxy, default is False
    :return: If JSON available: The fetches JSON file as dict, else: None
    """
    log = logging.getLogger(__name__)
    partitionkey = link.split('/')[-1]
    proxy, useragent = random.choice(proxylist)

    while True:
        try:
            if useproxy:
                response = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy},
                                        timeout=30)
                response.raise_for_status()
                log.info(f'{partitionkey}: Website retrieved after {response.elapsed}')
                break
            else:
                response = requests.get(link, timeout=30)
                response.raise_for_status()
                log.info(f'{partitionkey}: Website retrieved after {response.elapsed}')
                break
        except requests.exceptions.HTTPError as ex:
            if ex.response.status_code == 404:
                log.info(f'{partitionkey}: Requested site "Not Found" (404 Error)')
                return None
            elif ex.response.status_code == 429:
                log.info(f'{partitionkey}: JSON Retrieval too fast (429 Error)')
                time.sleep(15)
                continue
            else:
                # Raise all other HTTP errors for now to see what action is needed
                raise
        except requests.exceptions.ConnectionError:
            proxy, useragent = random.choice(proxylist)
            log.info(f'{partitionkey}: Connection Error occurred. Proxy has been changed to {proxy}')
            continue
        except requests.exceptions.Timeout:
            proxy, useragent = random.choice(proxylist)
            log.info(f'{partitionkey}: Timeout Error occurred. Proxy has been changed to {proxy}')
            continue

    instagram_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', response.text)

    return instagram_json


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


def write_json_local(fetched_json,
                     storage_directory,
                     storage_location,
                     partitionkey,
                     capturing_time,
                     subfolder=True):
    log = logging.getLogger(__name__)
    if subfolder:
        file_storage_json = os.path.join(storage_directory,
                                         storage_location,
                                         str(partitionkey))
    else:
        file_storage_json = os.path.join(storage_directory,
                                         storage_location)
    if not os.path.exists(file_storage_json):
        log.info(f'Local file storage: {file_storage_json} does not exist')
        os.makedirs(file_storage_json)
        log.info(f'Local file storage: {file_storage_json} has been created')
    try:
        with open(f'{file_storage_json}/{capturing_time}_{partitionkey}.json', 'w') as file:
            file.write(fetched_json[0])
        log.debug(f'{partitionkey}: Written to local file storage {storage_location}')
    except:
        log.exception(f'{partitionkey}: File could not be written, check for error.')
        raise


def write_json_s3(storage_link,
                  storage_bucket,
                  storage_directory,
                  storage_location,
                  partitionkey,
                  capturing_time,
                  subfolder=True):
    log = logging.getLogger(__name__)
    if subfolder:
        local_file_storage_json = os.path.join(storage_directory,
                                               storage_location,
                                               str(partitionkey))
        remote_file_storage_json = os.path.join(storage_location,
                                                str(partitionkey))
    else:
        local_file_storage_json = os.path.join(storage_directory,
                                               storage_location)
        remote_file_storage_json = os.path.join(storage_location)
    storage_link.upload_file(f'{local_file_storage_json}/{capturing_time}_{partitionkey}.json',
                             storage_bucket,
                             f'{remote_file_storage_json}/{capturing_time}_{partitionkey}.json'
                             )


def write_json(file_directory,
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

    # Save JSON locally
    if not os.path.exists(file_directory):
        log.info('Local file storage: %s does not exist', file_directory)
        os.makedirs(file_directory)
        log.info('Local file storage: %s has been created', file_directory)
    try:
        with open('{}/{}.json'.format(file_directory, keyid), 'w') as file:
            file.write(datetime.now().strftime('%s') + '\n')
            file.write(fetchedjson[0])
        log.debug('%s: Written to local file storage %s', keyid, file_directory)
    except:
        log.exception('%s could not be written, check for error.', keyid)
        raise
    log.debug('%s: Uploading to S3 storage to %s started', keyid, s3_directory)
    s3_link.upload_file('{}/{}.json'.format(file_directory, keyid), 'gvbinsta-test',
                        '{}/{}.json'.format(s3_directory, keyid))
    log.debug('%s: Uploaded to S3 storage to %s', keyid, s3_directory)


def set_retrieved_time(db_link, key, value, capture_time):
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
                ':rtime': capture_time
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


class Retrieve:
    """
    Retrieve class extracts the JSON from Instagram, downloads the picture for posts,
    saves it locally and uploads it to Amazon S3 storage

    Todo: Creating a TOML file with the details about proxy, awsprofile, storage-folders and DBs
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
        self.s3 = True
        self.s3_bucket = 'gvbinsta-test'
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

    def retrieve_location(self, location_id):
        """
        Retrieve location details
        :param location_id: Location ID
        :return: True if JSON was retrieved; False if not
        """
        link = f'https://www.instagram.com/explore/locations/{location_id}/'
        with measure_time(location_id, 'Fetching JSON'):
            fetchedjson = get_json_instagram(link,
                                             self.proxies,
                                             self.useproxy)
        if not fetchedjson:
            self.log.info(f'{location_id}: JSON could not be retrieved from {link}')
            set_deleted(self.locdb, 'id', location_id)
            return False

        # Writing JSON to local directory
        discovered_at_time = int(time.time())
        with measure_time(location_id, 'Writing JSON locally'):
            write_json_local(fetchedjson,
                             self.storage_directory,
                             self.storage_json_location,
                             location_id,
                             discovered_at_time)
        self.log.debug(f'{location_id}: JSON file written to local directory')

        with measure_time(location_id, 'Updating DB about writing JSON locally'):
            set_retrieved_time(self.locdb,
                               'id',
                               location_id,
                               discovered_at_time)

        # Writing JSON to S3 storage
        if self.s3:
            with measure_time(location_id, 'Writing JSON to S3'):
                write_json_s3(self.s3_link,
                              self.s3_bucket,
                              self.storage_directory,
                              self.storage_json_location,
                              location_id,
                              discovered_at_time)

        # Completing
        self.log.debug(f'{location_id}: JSON has been saved')

        return True

    def retrieve_user(self, userid):
        """
        Retrieve user details
        :param userid: User ID
        :return: True if JSON was retrieved; False if not
        """
        link = f'https://www.instagram.com/{userid}/'
        with measure_time(userid, 'Fetching JSON'):
            fetchedjson = get_json_instagram(link,
                                             self.proxies,
                                             self.useproxy)
        if not fetchedjson:
            self.log.debug(f'{userid}: JSON could not be retrieved from {link}')
            set_deleted(self.userdb, 'username', userid)
            return False

        # Writing JSON to local directory
        discovered_at_time = int(time.time())
        with measure_time(userid, 'Writing JSON locally'):
            write_json_local(fetchedjson,
                             self.storage_directory,
                             self.storage_json_user,
                             userid,
                             discovered_at_time)
        self.log.debug(f'{userid}: JSON file written to local directory')

        with measure_time(userid, 'Updating DB about writing JSON locally'):
            set_retrieved_time(self.userdb,
                               'username',
                               userid,
                               discovered_at_time)

        # Writing JSON to S3 storage
        if self.s3:
            with measure_time(userid, 'Writing JSON to S3'):
                write_json_s3(self.s3_link,
                              self.s3_bucket,
                              self.storage_directory,
                              self.storage_json_user,
                              userid,
                              discovered_at_time)

        # Completing
        self.log.debug(f'{userid}: JSON has been saved')

        return True

    def retrieve_picture(self, pictureid):
        """
        Retrieve picture details
        :param pictureid: Picture ID
        :return: True if JSON was retrieved; False if not
        """
        link = f'https://www.instagram.com/p/{pictureid}/'
        with measure_time(pictureid, 'Fetching JSON'):
            fetchedjson = get_json_instagram(link,
                                             self.proxies,
                                             self.useproxy)
        if not fetchedjson:
            self.log.info(f'{pictureid}: JSON could not be retrieved from {link}')
            set_deleted(self.picdb, 'shortcode', pictureid)
            return False

        # Writing JSON to local directory
        discovered_at_time = int(time.time())
        with measure_time(pictureid, 'Writing JSON locally'):
            write_json_local(fetchedjson,
                             self.storage_directory,
                             self.storage_json_post,
                             pictureid,
                             discovered_at_time)
        self.log.debug(f'{pictureid}: JSON file writte to local directory')

        with measure_time(pictureid, 'Updating DB about writing JSON locally'):
            set_retrieved_time(self.picdb,
                               'shortcode',
                               pictureid,
                               discovered_at_time)

        # Writing JSON to S3 storage
        if self.s3:
            with measure_time(pictureid, 'Writing JSON to S3'):
                write_json_s3(self.s3_link,
                              self.s3_bucket,
                              self.storage_directory,
                              self.storage_json_post,
                              pictureid,
                              discovered_at_time)

        # Writing Picture to S3 storage

        # Completing
        self.log.debug(f'{pictureid}: JSON has been saved')

        #Todo: extract grabimage even further as done with JSON
        file_storage_pictures = os.path.join(self.storage_directory, self.storage_pictures)
        try:
            grabimage(file_storage_pictures,
                      self.s3_link,
                      self.storage_pictures,
                      pictureid,
                      fetchedjson[0],
                      random.choice(self.proxies),
                      self.useproxy)
        except Exception:
            self.log.exception('Check the exception with the following: %s, %s',
                               pictureid, fetchedjson)

        return True
