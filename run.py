import logging
import boto3
import multiprocessing as mp

from app import Retrieve
from app import Search
from app import Extract

def mp_retrieve_location(location):
    retr.retrieve_location(location)

def mp_retrieve_picture(picture):
    retr.retrieve_picture(picture)

def mp_retrieve_user(user_list):
    log = logging.getLogger(__name__)
    number, user = user_list
    log.info('#%s: %s - Retrieving data from Instagram',number,user['username'])
    retr.retrieve_user(user['username'])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Initialize profiles
    sr = Search()
    retr = Retrieve(useproxy=True, awsprofile='default')
    ex = Extract(awsprofile='default')
    dynamo = boto3.resource('dynamodb')
    tbl_user = dynamo.Table('test4')



    pool = mp.Pool(processes=20)

    mode = 'one-off_users'

    # one-off retrieve and extract locations
    if mode == 'one-off_location':
        retrlocations = sr.incomplete(category='location',
                                  step='discovered',
                                  getitems=1000)
        pool.map(mp_retrieve_location, retrlocations)
        extrlocations = sr.incomplete(category='location',
                                  step='retrieved')
        for location in extrlocations:
            logging.info('%s: Starting with location', location)
            ex.location_details(location)

    # one-off retrieve and extract pictures
    if mode == 'one-off_pictures':
        retrpictures = sr.incomplete(category='picture',
                                 step='discovered',
                                 getitems=1000)
        pool.map(mp_retrieve_picture, retrpictures)
        extrpictures = sr.incomplete(category='picture',
                                     step='retrieved')
        for picture in extrpictures:
            logging.info('%s: Starting with picture', picture)
            ex.picture_details(picture)

    # one-off retrieve and extract users

    if mode == 'one-off_users':

        response = sr.scan_key_with_filter(tbl_user,
                                           'username',
                                           'discovered')
        retrusers = list(enumerate(response, 1))
        pool.map(mp_retrieve_user, retrusers)
        # extrusers = sr.incomplete(category='user',
        #                           step='retrieved')
        # for user in extrusers:
        #     logging.info('%s: Starting with user', user)
        #     ex.user_details(user)


    # weekly


    # all five minutes