import logging

from app import Retrieve
from app import Search
from app import Extract

logging.basicConfig(level=logging.INFO)

sr = Search()
retr = Retrieve(useproxy=True, awsprofile='default')
ex = Extract(awsprofile='default')
