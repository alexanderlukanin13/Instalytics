import logging
import multiprocessing as mp

from app import Retrieve
from app import Search
from app import Extract

import requests

logging.basicConfig(level=logging.INFO)

sr = Search()
retr = Retrieve(useproxy=True, awsprofile='default')
ex = Extract(awsprofile='default')

# list = sr.incomplete(category='user', step='all', getitems=100000)
#
# def mp_retrieve(user):
#     retr.retrieve_user(user)
#
# pool = mp.Pool(processes=10)
#
# pool.map(mp_retrieve, list)

# list = sr.incomplete(category='picture')
#
# logging.info(list)
# logging.info(len(list))
#
# for post in list:
# # for post in list[0:1]:
#     try:
#         retr.retrieve_picture(post)
#         ex.picture_details(post)
#         logging.info('%s completed', post)
#     except FileNotFoundError:
#         logging.info('%s file not found', post)

#pictures = sr.incomplete('picture', 'retrieved')
#print(pictures)

#retr.retrieve_picture('BNKBq6LAzjq')

#response = retr.retrieve_location(1763118290572140) #404 Example
response = retr.retrieve_location(1032993860) #Normal example

if response == False:
    logging.info('ID could not been retrieved')

if response == True:
    logging.info('ID has been processed')

# retr.retrieve_user('_mo.duinne_')
# ex.user_details('_mo.duinne_')

# for location in ['1664544623601787']:
#     retr.retrieve_location(location)
#     ex.location_details(location)

# for post in ['BNKBq6LAzjq']:
#     retr.retrieve_picture(post)
#     ex.picture_details(post)

# for user in ['world_besttravel']:
#     retr.retrieve_user(user)
#     ex.user_details(user)