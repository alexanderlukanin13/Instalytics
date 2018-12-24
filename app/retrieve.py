import logging
import random
import requests
import re
import shutil
import boto3

from datetime import datetime

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

def grabjson(link, chosenproxy):

    proxy, useragent = chosenproxy

    resp = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy}, timeout=60)

    resp_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', resp.text)

    return resp_json

def writejson(id, fetchedjson, s3):

    with open('./downloads/json/{}.json'.format(id), 'w') as f:
        f.write(datetime.now().strftime('%s') + '\n')
        f.writelines(fetchedjson)

    s3.upload_file('./downloads/json/{}.json'.format(id), 'gvbinsta-test',
                        'json/{}.json'.format(id))


class Retrieve:

    """
    Retrieve class retrieves the JSON from Instagram
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.proxies = proxies_file()
        self.s3 = boto3.client('s3')
        self.dynamo = boto3.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.userdb = self.dynamo.Table('test4')

    def retrieve_location(self, locationid):

        link = 'https://www.instagram.com/explore/locations/{}'.format(locationid)

        fetchedjson = grabjson(link, random.choice(self.proxies))

        writejson(locationid, fetchedjson, self.s3)

        response = self.locdb.update_item(
            Key={
                'id': int(locationid)
            },
            UpdateExpression='set retrieved_at_time = :retrtime',
            ExpressionAttributeValues={
                ':retrtime': int(datetime.now().strftime('%s'))
            }
        )

    def retrieve_user(self, userid):

        link = 'https://www.instagram.com/{}/'.format(userid)

        fetchedjson = grabjson(link, random.choice(self.proxies))

        writejson(userid, fetchedjson, self.s3)

        response = self.userdb.update_item(
            Key={
                'username': userid
            },
            UpdateExpression='set retrieved_at_time = :retrtime',
            ExpressionAttributeValues={
                ':retrtime': int(datetime.now().strftime('%s'))
            }
        )

    def retrieve_picture(self, pictureid):

        link = 'https://www.instagram.com/p/{}/'.format(pictureid)

        proxy, useragent = random.choice(self.proxies)

        resp = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy}, timeout=60)

        resp_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', resp.text)

        with open('./downloads/json/{}.json'.format(pictureid), 'w') as f:
            f.write(datetime.now().strftime('%s') + '\n')
            f.writelines(resp_json)

        self.s3.upload_file('./downloads/json/{}.json'.format(pictureid), 'gvbinsta-test',
                            'json/{}.json'.format(pictureid))

        imagelink = re.findall(r'"display_url":"([^"]+)"', resp.text)

        filename = imagelink[0].split('/')[-1].split('?')[0]

        imagefile = requests.get(imagelink[0], stream=True)

        with open('./downloads/picture/{}_{}'.format(pictureid, filename), 'wb') as f:
            shutil.copyfileobj(imagefile.raw, f)

        self.s3.upload_file('./downloads/picture/{}_{}'.format(pictureid, filename), 'gvbinsta-test',
                'pictures/{}_{}'.format(pictureid, filename))

        response = self.picdb.update_item(
            Key={
                'shortcode': pictureid
            },
            UpdateExpression='set retrieved_at_time = :retrtime',
            ExpressionAttributeValues={
                ':retrtime': int(datetime.now().strftime('%s'))
            }
        )