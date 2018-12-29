import logging
import boto3
import os

from boto3.dynamodb.conditions import Attr
from botocore import errorfactory


def readlastkey(category):

    try:
        with open('./tmp/{}-lastkey.tmp'.format(category), 'r') as f:
            lastkey = f.readline()
        return lastkey

    except FileNotFoundError:
        lastkey = None
        return lastkey


def savelastkey(category, lastkey):

    with open('./tmp/{}-lastkey.tmp'.format(category), 'w') as f:
        f.writelines(lastkey)


class Search:

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.dynamo = boto3.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.userdb = self.dynamo.Table('test4')

    def incomplete(self,
                   category='picture',
                   step='discovered',
                   getitems=1000):

        retrieveditems = 0
        scanneditems = 0
        itemslist = []

        lastkey = readlastkey(category)
        print(lastkey)

        while retrieveditems < getitems:

            if lastkey != None:
                try:
                    if category == 'picture' and step == 'discovered':
                        newresponse = self.picdb.scan(
                            FilterExpression=Attr('retrieved_at_time').not_exists(),
                            ExclusiveStartKey={'shortcode': lastkey}
                        )

                    elif category == 'picture' and step == 'retrieved':
                        newresponse = self.picdb.scan(
                            FilterExpression=Attr('processed_at_time').not_exists() &
                                             Attr('retrieved_at_time').exists(),
                            ExclusiveStartKey={'shortcode': lastkey}
                        )

                    #elif category == 'user' and step == 'discovered':

                    #elif category == 'user' and step == 'retrieved':

                    #elif category == 'location' and step == 'discovered':

                    #elif category == 'location' and step == 'retrieved':

                    retrieveditems += newresponse['Count']
                    scanneditems += newresponse['ScannedCount']
                    self.log.info('{} items received from {} scanned'.format(retrieveditems, scanneditems))
                except errorfactory.ClientError as e:
                    self.log.exception('Dynamodb client error')
                    continue

                for item in newresponse['Items']:
                    itemslist.append(item)

                try:
                    lastkey = newresponse['LastEvaluatedKey']
                    savelastkey(category, lastkey)
                    self.log.debug('LastEvaluatedKey is {}, {} items retrieved'.format(lastkey, retrieveditems))

                except KeyError as e:
                    os.remove('./tmp/{}-lastkey.tmp'.format(category))
                    self.log.info('DB scan completed. No further LastEvaluatedKey')
                    break

            if lastkey == None:
                try:
                    if category == 'picture' and step == 'discovered':
                        response = self.picdb.scan(
                            FilterExpression=Attr('retrieved_at_time').not_exists()
                        )

                    elif category == 'picture' and step == 'retrieved':
                        response = self.picdb.scan(
                            FilterExpression=Attr('processed_at_time').not_exists() &
                                             Attr('retrieved_at_time').exists(),
                        )

                    retrieveditems += response['Count']
                    scanneditems += response['ScannedCount']
                    self.log.info('{} items received from {} scanned'.format(retrieveditems, scanneditems))
                except errorfactory.ClientError as e:
                    self.log.exception('Dynamodb client error')
                    continue

                itemslist = response['Items']

                try:
                    lastkey = response['LastEvaluatedKey']
                    self.log.debug('LastEvaluatedKey is {}, {} items retrieved'.format(lastkey, retrieveditems))
                except KeyError as e:
                    self.log.info('Item "LastEvaluatedKey" not available. DB too small. Processing normally')
                    break

        returnlist = []

        if category == 'picture':
            for item in itemslist:
                returnlist.append(item['shortcode'])

        # if category == 'user':

        # if category == 'location':

        return returnlist