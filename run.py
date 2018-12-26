import logging

from app import Retrieve
from app import Search
from app import Extract

logging.basicConfig(level=logging.INFO)

#sr = Search()

#pictures = sr.incomplete('picture', 'retrieved')
#print(pictures)

retr = Retrieve()

#retr.retrieve_picture('BHdAPvzBJMT')

#retr.retrieve_location('243768061')

#retr.retrieve_user('swissglam')

ex = Extract()

for location in ['1664544623601787']:
    retr.retrieve_location(location)
    ex.location_details(location)