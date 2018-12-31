import logging
import json
import os
import boto3

from datetime import datetime
from decimal import Decimal
from botocore import errorfactory


def tag_extractor(text, category, keytag):

    # category can be 'edge_media_to_caption' or 'edge_media_to_comment'

    tag_list = set()
    for comments in text[category]['edges']:
        if keytag in comments['node']['text']:
            splitted_tag_list = comments['node']['text'].split()
            for tag in splitted_tag_list:
                if keytag in tag:
                    subtag_list = tag.split(keytag)
                    for subtag in subtag_list:
                        tag_list.add(subtag)
    tag_list.discard('')

    return tag_list

class Extract:

    def __init__(self,
                 awsprofile='default',
                 awsregion='eu-central-1',
                 storage_directory='.'):

        self.log = logging.getLogger(__name__)
        self.awssession = boto3.session.Session(profile_name=awsprofile, region_name=awsregion)
        self.s3 = self.awssession.client('s3')
        self.dynamo = self.awssession.resource('dynamodb')
        self.picdb = self.dynamo.Table('test2')
        self.locdb = self.dynamo.Table('test3')
        self.locdbupdate = self.dynamo.Table('test3-1')
        self.userdb = self.dynamo.Table('test4')
        self.userdbupdate = self.dynamo.Table('test4-1')
        self.storage_directory = storage_directory
        self.storage_json_location = 'json/location'
        self.storage_json_user = 'json/user'
        self.storage_json_post = 'json/post'
        self.storage_pictures = 'pictures'

    def location_details(self, locationid):

        #Configure dictionary for saving the results

        location = {}

        #Get json from file
        file_storage_json_location = os.path.join(self.storage_directory, self.storage_json_location)
        with open('{}/{}.json'.format(file_storage_json_location, locationid), 'r') as f:
            filetext = f.read().split('\n')
            rawjson = filetext[1]
            retrieved_at_time = int(filetext[0])

        # Transform JSON from file

        rawdatastore = json.loads(rawjson)
        datastore = rawdatastore['entry_data']['LocationsPage'][0]['graphql']['location']

        # Extract location details from JSON

        locationdict = ['name', 'has_public_page', 'slug', 'blurb', 'website', 'phone',
                        'primary_alias_on_fb']

        for element in locationdict:
            if datastore[element] not in ['', None]:
                location[element] = datastore[element]

        # Extract location coordinates as Decimal

        locationcoordinates = ['lat', 'lng']

        for element in locationcoordinates:
            if datastore[element] not in ['', None]:
                location[element] = Decimal(str(datastore[element]))

        # Unpack and extract JSON location address details

        if datastore['address_json'] not in ['', None]:
            addressjson = json.loads(datastore['address_json'])

            addressdict = ['street_address', 'zip_code', 'city_name', 'region_name', 'country_code', 'exact_city_match',
                           'exact_region_match', 'exact_country_match']

            for element in addressdict:
                if addressjson[element] not in ['', None]:
                    location['json_' + element] = addressjson[element]

        # Extract location directory details from JSON

        try:
            countryjson = datastore['directory']['country']
            directorydict = ['id', 'name', 'slug']
            cityjson = datastore['directory']['city']

            for element in directorydict:
                if countryjson[element] not in ['', None]:
                    location['country_' + element] = countryjson[element]

            for element in directorydict:
                if cityjson[element] not in ['', None]:
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

        try:
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
                        else:
                            raise

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
        except KeyError:
            self.log.info('%s: Location has no geo coordinates', locationid)

    def picture_details(self, shortcode):

        # Configure dictionary for saving the results

        picture ={}

        # Get json from file
        try:
            file_storage_json_picture = os.path.join(self.storage_directory, self.storage_json_post)
            with open('{}/{}.json'.format(file_storage_json_picture, shortcode), 'r') as f:
                filetext = f.read().split('\n')
                rawjson = filetext[1]
                retrieved_at_time = int(filetext[0])

        except FileNotFoundError:
            self.log.debug('%s: File not found', shortcode)
            raise

        # Transform JSON from file

        rawdatastore = json.loads(rawjson)
        datastore = rawdatastore['entry_data']['PostPage'][0]['graphql']['shortcode_media']

        # Extract picture details from JSON - 1st level

        postlist = ['gating_info', 'display_url', 'accessibility_caption', 'is_video', 'should_log_client_event',
                    'caption_is_edited', 'has_ranked_comments', 'comments_disabled', 'taken_at_timestamp', 'is_ad']

        for element in postlist:
            try:
                if datastore[element] not in ['', None]:
                    picture[element] = datastore[element]
            except KeyError:
                self.log.debug('Picture {}: {} not available within post'.format(shortcode, element))

        picture['id'] = int(datastore['id'])

        # Extract picture details from JSON - Size details

        sizelist = ['height', 'width']

        for element in sizelist:
            if datastore['dimensions'][element] not in ['', None]:
                picture['size_' + element] = datastore['dimensions'][element]


        # Extract picture details from JSON - edge_media_to_tagged_user

        if len(datastore['edge_media_to_tagged_user']['edges']) > 0:
            tagged_users = set()

            for user in datastore['edge_media_to_tagged_user']['edges']:
                tagged_users.add(user['node']['user']['username'])

            picture['tagged_users'] = list(tagged_users)
            picture['tagged_users_count'] = len(tagged_users)

        else:
            picture['tagged_users_count'] = 0

        # Extract picture details from JSON - edge_media_to_caption

        try:
            picture['caption'] = datastore['edge_media_to_caption']['edges'][0]['node']['text']

        except IndexError:
            self.log.debug('Picture {}: No caption available'.format(shortcode))


        # Extract picture details from JSON - edge_media_to_comment

        if len(datastore['edge_media_to_comment']['edges']) > 0:
            picture_comments_commenters = set()

            for user in datastore['edge_media_to_comment']['edges']:
                picture_comments_commenters.add(user['node']['owner']['username'])

            picture['comments_count'] = datastore['edge_media_to_comment']['count']
            picture['comments_has_next_page'] = datastore['edge_media_to_comment']['page_info']['has_next_page']
            picture['commenters'] = list(picture_comments_commenters)
            picture['commenters_count'] = len(picture_comments_commenters)

        else:
            picture['comments_count'] = 0
            picture['commenters_count'] = 0
            picture['comments_has_next_page'] = False

        # Extract picture details from JSON - Owner and did he/she answer comments

        picture['owner'] = datastore['owner']['username']
        picture['ownerid'] = int(datastore['owner']['id'])

        try:
            if picture['owner'] in picture['commenters']:
                picture['owner_commented'] = True

        except KeyError:
            picture['owner_comments'] = False

        # Extract picture details from JSON - edge_media_preview_like

        picture['likes_count'] = datastore['edge_media_preview_like']['count']

        # Extract picture details from JSON - edge_media_to_sponsor_user

        try:
            picture['sponsor'] = datastore['edge_media_to_sponsor_user']['edges'][0]['node']['sponsor']['username']

        except IndexError:
            self.log.debug('Picture {}: No sponsor found')

        # Extract picture details from JSON - location

        try:
            picture['location_id'] = int(datastore['location']['id'])

        except TypeError:
            self.log.debug('Picture {}: No location available'.format(shortcode))

        # Extract picture details from JSON - time between "retrieved" and picture posted

        picture['time_passed'] = retrieved_at_time - picture['taken_at_timestamp']

        # Extract picture details from JSON - Extracted Hashtags

        picture_ht_list = set()

        # -- Find hashtags or user references in the picture caption

        if len(datastore['edge_media_to_caption']['edges']) > 0:

            caption_tag_list = tag_extractor(datastore, 'edge_media_to_caption', '#')

            for tag in caption_tag_list:
                picture_ht_list.add(tag)

        # -- Find hashtags or user references in the comments

        if len(datastore['edge_media_to_comment']['edges']) > 0:

            media_tag_list = tag_extractor(datastore, 'edge_media_to_comment', '#')

            for tag in media_tag_list:
                picture_ht_list.add(tag)

        self.log.debug(picture_ht_list)

        if len(picture_ht_list) > 0:
            picture['hashtags'] = list(picture_ht_list)
            picture['hashtags_count'] = len(picture_ht_list)
        else:
            picture['hashtags_count'] = 0

        # Extract picture details from JSON - Referenced users

        picture_ref_list = set()

        # -- Find hashtags or user references in the picture caption

        if len(datastore['edge_media_to_caption']['edges']) > 0:

            caption_tag_list = tag_extractor(datastore, 'edge_media_to_caption', '@')

            for tag in caption_tag_list:
                picture_ref_list.add(tag)

        # -- Find hashtags or user references in the comments

        if len(datastore['edge_media_to_comment']['edges']) > 0:

            media_tag_list = tag_extractor(datastore, 'edge_media_to_comment', '@')

            for tag in media_tag_list:
                picture_ref_list.add(tag)

        self.log.debug(picture_ref_list)

        if len(picture_ref_list) > 0:
            picture['referenced_users'] = list(picture_ref_list)
            picture['referenced_users_count'] = len(picture_ref_list)
        else:
            picture['referenced_users_count'] = 0

        # Extract picture details from JSON - When was it processed

        picture['processed_at_time'] = int(datetime.now().strftime('%s'))

        # Update into DB

        for key in picture.keys():
            self.log.debug('For picture {} update {} to {}'.format(shortcode, key, picture[key]))
            self.picdb.update_item(
                Key={
                    'shortcode': shortcode
                },
                UpdateExpression = 'SET #key = :value',
                ExpressionAttributeNames = {
                    '#key': key
                },
                ExpressionAttributeValues = {
                    ':value': picture[key]
                },
            )

        # Extract location details & timestamp
        try:
            location = int(datastore['location']['id'])

            self.locdb.put_item(
                Item={
                    'id': location,
                     'discovered_at_time': retrieved_at_time
                },
                ConditionExpression='attribute_not_exists(id)'
            )

        except TypeError:
            self.log.info('%s: No location availabe', shortcode)

        except errorfactory.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.log.debug('Entry {} already exists in the database'.format(shortcode))

            else:
                raise


        # Extract user details & timestamp
        try:
            username = datastore['owner']['username']
            userid = int(datastore['owner']['id'])

            self.userdb.put_item(
                Item={
                    'username': username,
                    'userid': userid,
                    'discovered_at_time': retrieved_at_time
                },
                ConditionExpression='attribute_not_exists(username)'
            )

        except errorfactory.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.log.debug('%s: Entry already exists in the database', shortcode)
            else:
                raise


    def user_details(self, username):

        # Configure dictionary for saving the results

        user = {}

        # Get json from file
        file_storage_json_user= os.path.join(self.storage_directory, self.storage_json_user)
        with open('{}/{}.json'.format(file_storage_json_user, username), 'r') as f:
            filetext = f.read().split('\n')
            rawjson = filetext[1]
            retrieved_at_time = int(filetext[0])

        # Transform JSON from file

        rawdatastore = json.loads(rawjson)
        datastore = rawdatastore['entry_data']['ProfilePage'][0]['graphql']['user']

        # Extract user details from JSON

        userlist = ['biography', 'business_category_name', 'business_email', 'business_phone_number',
                    'connected_fb_page', 'country_block', 'external_url', 'full_name', 'has_channel',
                    'highlight_reel_count', 'is_business_account', 'is_joined_recently', 'is_private',
                    'is_verified', 'profile_pic_url_hd']

        for element in userlist:
            try:
                if datastore[element] not in ['', None]:
                    user[element] = datastore[element]
            except KeyError:
                self.log.debug('User {}: {} not available within post'.format(user, element))

        user['id'] = int(datastore['id'])

        # Extract user details from JSON - business details

        if datastore['business_address_json'] not in ['', None]:
            busaddrjson = json.loads(datastore['business_address_json'])

            baddrlist = ['street_address', 'zip_code', 'city_name', 'region_name', 'country_code']

            for element in baddrlist:
                if busaddrjson[element] not in ['', None]:
                    user['json_' + element] = busaddrjson[element]


        # Extract user details from JSON - follower, follow & posts detail

        user['follow_count'] = datastore['edge_follow']['count']
        user['followed_by_count'] = datastore['edge_followed_by']['count']
        user['posts_count'] = datastore['edge_owner_to_timeline_media']['count']

        user['processed_at_time'] = int(datetime.now().strftime('%s'))

        # Update into DB

        for key in user.keys():
            self.log.debug('For user {} update {} to {}'.format(user, key, user[key]))
            self.userdb.update_item(
                Key={
                    'username': username
                },
                UpdateExpression = 'SET #key = :value',
                ExpressionAttributeNames = {
                    '#key': key
                },
                ExpressionAttributeValues = {
                    ':value': user[key]
                },
            )

        # Extract picture details & timestamp

        for picture in datastore['edge_owner_to_timeline_media']['edges']:

            try:
                self.picdb.put_item(
                    Item={
                        'shortcode': picture['node']['shortcode'],
                        'userid': int(picture['node']['owner']['id']),
                        'discovered_at_time': retrieved_at_time
                    },
                    ConditionExpression='attribute_not_exists(username)'
                )

            except errorfactory.ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    self.log.debug('Entry {} already exists in the database'.format(picture))
                else:
                    raise

        # Weekly snapshot

        self.userdbupdate.put_item(
            Item={
                'username': username,
                'at_time': int(datetime.now().strftime('%s')),
                'id': int(datastore['id']),
                'follow_count': user['follow_count'],
                'followed_by_count': user['followed_by_count'],
                'posts_count': user['posts_count']
            }
        )