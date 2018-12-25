from app import Retrieve
from app import Search

sr = Search()

pictures = sr.incomplete('picture', 'retrieved')
print(pictures)

#retr = Retrieve()

#retr.retrieve_picture('BHdAPvzBJMT')

#retr.retrieve_location('243768061')

#retr.retrieve_user('swissglam')
