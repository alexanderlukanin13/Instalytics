import logging

from app import Retrieve
from app import Search
from app import Extract

logging.basicConfig(level=logging.INFO)

sr = Search()
retr = Retrieve()
ex = Extract()

#pictures = sr.incomplete('picture', 'retrieved')
#print(pictures)

#retr.retrieve_picture('BHdAPvzBJMT')

#retr.retrieve_location('243768061')

#retr.retrieve_user('swissglam')


# for location in ['1664544623601787']:
#     retr.retrieve_location(location)
#     ex.location_details(location)

# for post in ['BNKBq6LAzjq']:
#     retr.retrieve_picture(post)
#     ex.picture_details(post)

for user in ['world_besttravel']:
    retr.retrieve_user(user)
    ex.user_details(user)