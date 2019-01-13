import logging
import boto3
import os
import json

from boto3.dynamodb.conditions import Attr
from botocore import errorfactory


def readlastkey(category, step):

    try:
        with open('./tmp/{}-{}-lastkey.tmp'.format(category, step), 'r') as f:
            lastkey = json.loads(f.read())
        return lastkey

    except FileNotFoundError:
        lastkey = None
        return lastkey


def savelastkey(category, step, lastkey):
    lastkeyjson = json.dumps(lastkey)
    with open('./tmp/{}-{}-lastkey.tmp'.format(category, step), 'w') as f:
        f.write(lastkeyjson)

class Search:

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.dynamo = boto3.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.userdb = self.dynamo.Table('test4')

    def scan_key_with_filter(self,
                             db,
                             key,
                             used_filter,
                             items=1000):

        filter_expressions = {
            'all': Attr('deleted').not_exists(),
            'discovered': Attr('retrieved_at_time').not_exists() &
                          Attr('deleted').not_exists(),
            'retrieved': Attr('processed_at_time').not_exists() &
                         Attr('deleted').not_exists() &
                         Attr('retrieved_at_time').exists()
        }

        resultlist = []
        retritem = 0
        scanneditem = 0
        consumedcapacity = 0
        lastkey = None

        while retritem < items:
            if lastkey != None:
                response = db.scan(
                    ProjectionExpression=key,
                    FilterExpression=filter_expressions[used_filter],
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey=lastkey
                )
            if lastkey == None:
                response = db.scan(
                    ProjectionExpression=key,
                    FilterExpression=filter_expressions[used_filter],
                    ReturnConsumedCapacity='TOTAL'
                )

            resultlist.extend(response['Items'])
            retritem += response['Count']
            scanneditem += response['ScannedCount']
            consumedcapacity += response['ConsumedCapacity']['CapacityUnits']
            self.log.info('%s out of %s DB items received', retritem, scanneditem)

            try:
                lastkey = response['LastEvaluatedKey']
            except Exception:
                self.log.info('After %s scanned itmens. No more last keys', scanneditem)
                break

        return resultlist

    def incomplete(self,
                   category=None,
                   step='discovered',
                   getitems=1000):

        retrieveditems = 0
        scanneditems = 0
        itemslist = []

        lastkey = readlastkey(category, step)

        if category == 'picture':
            db = self.picdb
        elif category == 'location':
            db = self.locdb
        elif category == 'user':
            db = self.userdb
        elif category == None:
            raise ValueError('Specify the category')
        else:
            raise ValueError('Wrong category chosen')

        dbkey = {'picture': 'shortcode',
                 'location': 'id',
                 'user': 'username'}

        while retrieveditems < getitems:

            if lastkey != None:
                try:
                    if step == 'discovered':
                        newresponse = db.scan(
                            FilterExpression=Attr('retrieved_at_time').not_exists() &
                                             Attr('deleted').not_exists(),
                            ExclusiveStartKey={dbkey[category]: lastkey[dbkey[category]]}
                        )

                    elif step == 'retrieved':
                        newresponse = db.scan(
                            FilterExpression=Attr('processed_at_time').not_exists() &
                                             Attr('deleted').not_exists() &
                                             Attr('retrieved_at_time').exists(),
                            ExclusiveStartKey={dbkey[category]: lastkey[dbkey[category]]}
                        )

                    elif step == 'all':
                        newresponse = db.scan(
                            ExclusiveStartKey={dbkey[category]: lastkey[dbkey[category]]}
                        )

                    retrieveditems += newresponse['Count']
                    scanneditems += newresponse['ScannedCount']
                    self.log.info('{} items received from {} scanned'.format(retrieveditems, scanneditems))
                except errorfactory.ClientError:
                    self.log.exception('Dynamodb client error')
                    continue

                for item in newresponse['Items']:
                    itemslist.append(item)

                try:
                    lastkey = newresponse['LastEvaluatedKey']
                    savelastkey(category, step, lastkey)
                    self.log.debug('LastEvaluatedKey is {}, {} items retrieved'.format(lastkey, retrieveditems))

                except KeyError:
                    os.remove('./tmp/{}-{}-lastkey.tmp'.format(category, step))
                    self.log.info('DB scan completed. No further LastEvaluatedKey')
                    break

            if lastkey == None:
                try:
                    if step == 'discovered':
                        response = db.scan(
                            FilterExpression=Attr('retrieved_at_time').not_exists() &
                                             Attr('deleted').not_exists()
                        )

                    elif step == 'retrieved':
                        response = db.scan(
                            FilterExpression=Attr('processed_at_time').not_exists() &
                                             Attr('deleted').not_exists() &
                                             Attr('retrieved_at_time').exists(),
                        )
                    elif step == 'all':
                        response = db.scan()

                    retrieveditems += response['Count']
                    scanneditems += response['ScannedCount']
                    self.log.info('{} items received from {} scanned'.format(retrieveditems, scanneditems))
                except errorfactory.ClientError:
                    self.log.exception('Dynamodb client error')
                    continue

                itemslist = response['Items']

                try:
                    lastkey = response['LastEvaluatedKey']
                    savelastkey(category, step, lastkey)
                    self.log.debug('LastEvaluatedKey is {}, {} items retrieved'.format(lastkey, retrieveditems))
                except KeyError:
                    self.log.info('Item "LastEvaluatedKey" not available. DB too small. Processing normally')
                    break

        returnlist = []

        for item in itemslist:
            returnlist.append(item[dbkey[category]])

        return returnlist[:getitems]