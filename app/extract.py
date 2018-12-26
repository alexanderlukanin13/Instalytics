import logging
import json
import boto3

from datetime import datetime
from decimal import Decimal
from botocore import errorfactory


class Extract:

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.dynamo = boto3.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.locdbupdate = self.dynamo.Table('test3-1')
        self.userdb = self.dynamo.Table('test4')

    def location_details(self, locationid):

        #Configure dictionary for saving the results

        location = {}

        #Get json from file

        with open('./downloads/json/{}.json'.format(locationid), 'r') as f:
            filetext = f.read().split('\n')
            rawjson = filetext[1]
            retrieved_at_time = int(filetext[0])

        #Transform JSON from file

        rawdatastore = json.loads(rawjson)
        datastore = rawdatastore['entry_data']['LocationsPage'][0]['graphql']['location']

        # Extract location details from JSON

        locationdict = ['name', 'has_public_page', 'slug', 'blurb', 'website', 'phone',
                        'primary_alias_on_fb']

        for element in locationdict:
            if datastore[element] != '':
                location[element] = datastore[element]

        # Extract location coordinates as Decimal

        locationcoordinates = ['lat', 'lng']

        for element in locationcoordinates:
            if datastore[element] != '':
                location[element] = Decimal(str(datastore[element]))

        # Unpack and extract JSON location address details

        if datastore['address_json'] != '':
            addressjson = json.loads(datastore['address_json'])

        addressdict = ['street_address', 'zip_code', 'city_name', 'region_name', 'country_code', 'exact_city_match',
                       'exact_region_match', 'exact_country_match']

        for element in addressdict:
            if addressjson[element] != '':
                location['json_' + element] = addressjson[element]

        # Extract location directory details from JSON

        try:
            countryjson = datastore['directory']['country']
            directorydict = ['id', 'name', 'slug']
            cityjson = datastore['directory']['city']

            for element in directorydict:
                if countryjson[element] != '':
                    location['country_' + element] = countryjson[element]


            for element in directorydict:
                if cityjson[element] != '':
                    location['city_' + element] = cityjson[element]
        except KeyError as e:
            self.log.info('Location {}: No directory details available'.format(locationid))

        # Extract further location details & timestamp

        location['media_count'] = datastore['edge_location_to_media']['count']

        location['processed_at_time'] = int(datetime.now().strftime('%s'))

        self.log.info('Location details for {} extracted'.format(locationid))

        # Update into DB

        self.log.debug('Location keys for location {}: {}'.format(locationid, location.keys()))

        for key in location.keys():
            self.log.debug('For location {} update {} to {}'.format(locationid, key, location[key]))
            self.locdb.update_item(
                Key={
                    'id': int(locationid)
                },
                UpdateExpression = 'SET #key = :value',
                ExpressionAttributeNames = {
                    '#key': key
                },
                ExpressionAttributeValues = {
                    ':value': location[key]
                },
            )

        self.log.info('Location {} saved to DB'.format(locationid))

        # Weekly snapshot

        self.locdbupdate.put_item(
            Item={
                'id': int(locationid),
                'at_time': int(datetime.now().strftime('%s')),
                'media_count': location['media_count']
            }
        )

        # Extract pictures when in Switzerland

        # Only extract picture from Switzerland
        # North 47.808463, 8.568019
        # South 45.817933, 9.017074
        # West 46.132250, 5.955882
        # East 46.615055, 10.492088

        if 5.955882 < location['lng'] < 10.492088 and 45.817933 < location['lat'] < 47.808463:

            pictures = []

            for pic in range(len(datastore['edge_location_to_media']['edges'])):
                pictures.append((datastore['edge_location_to_media']['edges'][pic]['node']['shortcode'],
                                datastore['edge_location_to_media']['edges'][pic]['node']['owner']['id']))

            self.log.debug(pictures)

        # Upload extracted pictures

            failedpictures = []
            uploadedpictures = []

            for pic in pictures:
                try:
                    self.picdb.put_item(
                        Item={
                            'shortcode': pic[0],
                            'userid': pic[1],
                            'discovered_at_time': retrieved_at_time
                        },
                        ConditionExpression = 'attribute_not_exists(#sc)',
                        ExpressionAttributeNames = {
                            '#sc': 'shortcode'
                        },
                    )
                    uploadedpictures.append(pic[0])
                except errorfactory.ClientError as e:
                    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                        failedpictures.append(pic[0])
                        self.log.debug('Entry {} already exists in the database'.format(pic))

            if len(uploadedpictures) > 0:
                self.log.info('For {} the following {} pictures were added: {}'.format(locationid,
                                                                                       len(uploadedpictures),
                                                                                       uploadedpictures))
            else:
                self.log.info('No new pictures from location {} extracted'.format(locationid))

            self.log.debug('The following pictures already exist in the DB: {}'.format(failedpictures))

        else:
            self.log.info('No pictures for location {} have been extracted as they are likely outside Switzerland'
                          .format(locationid))

    #def picture_details(self):

    #def user_details(self):