"""
This is the runner module which is the main module to run Instalytics
"""

import multiprocessing as mp
import logging
import argparse
import boto3

from app import Retrieve
from app import Search
from app import Extract


retr = None


def log_header(log_message):
    return f' {log_message} '.center(60, '=')


def mp_retrieve_location(location_list):
    """
    Function to support multiprocessing and avoid getting a pickle error for locations
    :param location_list: Tuple that contains a position number and the location dictionary,
    e.g. (1, {"id": 1992983})
    :return: None

    Todo: Adding the process id and picture number for an improved progress tracking
    """
    log = logging.getLogger(__name__)
    number, location_dictionary = location_list
    log.info('#%s: %s - Retrieving data from Instagram', number, location_dictionary['id'])
    retr.retrieve_location(location_dictionary['id'])

def mp_retrieve_picture(picture_list):
    """
    Function to support multiprocessing and avoid getting a pickle error for pictures
    :param picture_list: Tuple that contains a position number and the picture dictionary,
    e.g. (1, {'shortcode': '0gKBcODBtk'})
    :return: None

    Todo: Adding the process id and picture number for an improved progress tracking
    """
    log = logging.getLogger(__name__)
    number, picture_dictionary = picture_list
    log.info('#%s: %s - Retrieving data from Instagram', number, picture_dictionary['shortcode'])
    retr.retrieve_picture(picture_dictionary['shortcode'])

def mp_retrieve_user(user_list):
    """
    Function to support multiprocessing and avoid getting a pickle error for pictures
    :param user_list: Tuple that contains a position number and the user dictionary,
    e.g. (1, {'username': 'giorgio'}
    :return: None

    Todo: Adding the process id and picture number for an improved progress tracking
    """
    log = logging.getLogger(__name__)
    number, user_dictionary = user_list
    log.info('#%s: %s - Retrieving data from Instagram', number, user_dictionary['username'])
    retr.retrieve_user(user_dictionary['username'])


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='The Instalytics main program which runs pre-defined recipes to get, '
                                                 'extract, enrich and export Instagram data for analytics purposes')
    subparser = parser.add_subparsers()

    # Parser for running one-off searches / test
    parser_get = subparser.add_parser('get')
    parser_get.add_argument('category', choices=('location', 'user', 'post'),
                             help='Define the category that you want to search, e.g. location',)
    parser_get.add_argument('key', help='Give the key you want to search, e.g 39949930 (for location)')
    parser_get.set_defaults(command='get')

    # Parser for running the program
    parser_run = subparser.add_parser('run')
    parser_run.add_argument('category', choices=('location', 'user', 'post'))
    parser_run.set_defaults(command='run')

    # Parser for updating a category
    parser_update = subparser.add_parser('update')
    parser_update.add_argument('category', choices=('location', 'user', 'post'))
    parser_update.set_defaults(command='update')

    return parser.parse_args()


def main():
    """Main function of the script."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    logging.info(args.__dict__)

    # Initialize profiles
    sr = Search()
    global retr
    retr = Retrieve(useproxy=True, awsprofile='default', storage_directory='./downloads')
    ex = Extract(awsprofile='default', storage_directory='./downloads')
    dynamo = boto3.resource('dynamodb')
    tbl_user = dynamo.Table('te_user')
    tbl_pictures = dynamo.Table('te_post')
    tbl_locations = dynamo.Table('te_location')

    pool = mp.Pool()

    # one location
    if args.command == 'get' and args.category == 'location':
        logging.info('%s: Extracting location details', args.key)
        retr.retrieve_location(int(args.key))
        ex.location_details(args.key)

    # one pictures
    elif args.command == 'get' and args.category == 'post':
        logging.info('%s: Extracting picture details', args.key)
        retr.retrieve_picture(args.key)
        ex.picture_details(args.key)

    # one user
    elif args.command == 'get' and args.category == 'user':
        logging.info('%s: Extracting user details', args.key)
        retr.retrieve_user(args.key)
        ex.user_details(args.key)

    # run retrieve and extract locations
    elif args.command == 'run' and args.category == 'location':
        log_header('=== LOCATIONS - STARTING TO RETRIEVE FROM INSTAGRAM ===')
        response = sr.scan_key_with_filter(tbl_locations,
                                           'id',
                                           'discovered')
        retrlocations = list(enumerate(response, 1))
        pool.map(mp_retrieve_location, retrlocations)

        log_header('=== LOCATIONS - RETRIEVING FROM INSTAGRAM COMPLETED ===')

        log_header('=== LOCATIONS - EXTRACTING INFORMATION ===')

        extrlocations = sr.scan_key_with_filter(tbl_locations,
                                                'id',
                                                'retrieved')
        # extrlocations = sr.incomplete(category='location',
        #                               step='retrieved')
        extrlocations_total = len(extrlocations)
        for location in list(enumerate(extrlocations)):
            location_number, location_id = location
            logging.info('[%s]/[%s]: %s - Extracting location details',
                         location_number, extrlocations_total, location_id['id'])
            ex.location_details(location_id['id'])

        log_header('=== LOCATIONS - EXTRACTING COMPLETED ===')

    # run retrieve and extract pictures
    elif args.command == 'run' and args.category == 'post':
        log_header('=== PICTURES - STARTING TO RETRIEVE FROM INSTAGRAM ===')

        response = sr.scan_key_with_filter(tbl_pictures,
                                           'shortcode',
                                           'discovered',
                                           items=10)
        retrpictures = list(enumerate(response, 1))
        pool.map(mp_retrieve_picture, retrpictures)

        log_header('=== PICTURES - RETRIEVING FROM INSTAGRAM COMPLETED ===')

        log_header('=== PICTURES - EXTRACTING INFORMATION ===')

        extrpictures = sr.scan_key_with_filter(tbl_pictures,
                                               'shortcode',
                                               'retrieved')
        # extrpictures = sr.incomplete(category='picture',
        #                              step='retrieved')
        extrpictures_total = len(extrpictures)
        for picture in list(enumerate(extrpictures, 1)):
            picture_number, picture_name = picture
            logging.info('[%s]/[%s]: %s - Extracting user details',
                         picture_number, extrpictures_total, picture_name['shortcode'])
            ex.picture_details(picture_name['shortcode'])

        log_header('=== PICTURES - EXTRACTING COMPLETED ===')

    # run retrieve and extract users
    elif args.command == 'run' and args.category == 'user':

        log_header('=== USERS - STARTING TO RETRIEVE FROM INSTAGRAM ===')

        response = sr.scan_key_with_filter(tbl_user,
                                           'username',
                                           'discovered')
        retrusers = list(enumerate(response, 1))
        pool.map(mp_retrieve_user, retrusers)

        log_header('=== USERS - RETRIEVING FROM INSTAGRAM COMPLETED ===')

        log_header('=== USERS - EXTRACTING INFORMATION ===')

        extrusers = sr.scan_key_with_filter(tbl_user,
                                            'username',
                                            'retrieved')
        extrusers_total = len(extrusers)
        for user in list(enumerate(extrusers, 1)):
            user_number, user_name = user
            logging.info('[%s]/[%s]: %s - Extracting user details',
                         user_number, extrusers_total, user_name['username'])
            ex.user_details(user_name['username'])

        log_header('=== USERS - EXTRACTING COMPLETED ===')

    # weekly updating

    elif args.command == 'update' and args.category == 'location':

        logging.info(log_header('UPDATE LOCATION - RETR FROM INSTAGRAM STARTED'))
        response = sr.scan_key_with_filter(tbl_locations,
                                           'id',
                                           'all')
        retrlocations = list(enumerate(response, 1))
        pool.map(mp_retrieve_location, retrlocations)
        logging.info((log_header('UPDATE LOCATION - RETR FROM INSTAGRAM COMPLETED')))

        logging.info((log_header('UPDATE LOCATION - EXTRACTING INFORMATION')))
        extrlocations = sr.scan_key_with_filter(tbl_locations,
                                                'id',
                                                'all')
        extrlocations_total = len(extrlocations)
        for location in list(enumerate(extrlocations)):
            location_number, location_id = location
            logging.info('[%s]/[%s]: %s - Extracting location details',
                         location_number, extrlocations_total, location_id['id'])
            ex.location_details(location_id['id'])
        logging.info(log_header('UPDATE LOCATION - EXTRACTING COMPLETED'))

    # all five minutes

    else:
        logging.info('No valid category')


if __name__ == '__main__':
    main()
