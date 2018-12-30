import boto3
from boto3.dynamodb.conditions import Attr, Key
import logging

dynamo = boto3.resource('dynamodb')
tb = dynamo.Table('test2')


def scan_key_with_filter(db, key, filter_expression, items=1000):
    log = logging.getLogger(__name__)
    resultlist = []
    retritem = 0
    scanneditem = 0
    consumedcapacity = 0
    lastkey = None

    while retritem < items:
        if lastkey != None:
            response = db.scan(
                ProjectionExpression=key,
                FilterExpression=filter_expression,
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey=lastkey
            )
        if lastkey == None:
            response = db.scan(
                ProjectionExpression=key,
                FilterExpression=filter_expression,
                ReturnConsumedCapacity='TOTAL'
            )

        resultlist.extend(response['Items'])
        retritem += response['Count']
        scanneditem += response['ScannedCount']
        consumedcapacity += response['ConsumedCapacity']['CapacityUnits']
        log.info('%s out of %s DB items received', retritem, scanneditem)

        try:
            lastkey = response['LastEvaluatedKey']
        except:
            log.info('After %s scanned itmens. No more last keys', scanneditem)
            break

    return resultlist



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    filterexp = Key('commenters_count').gt(30)
    finale = scan_key_with_filter(tb, 'shortcode', filterexp, items=400)
    print(finale)