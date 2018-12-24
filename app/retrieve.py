import logging
import random
import requests
import re
import shutil

from datetime import datetime

def proxies_file():

    """Returns a list of proxies from a local file"""

    proxies = []

    with open('./config/agentstrings.conf', 'r') as f:
        useragent = f.read().splitlines()

    with open('./config/proxys.conf', 'r') as f:
        proxyfile = f.read().splitlines()
        for linenumber in range(len(proxyfile)):
            proxies.append((proxyfile[linenumber], random.choice(useragent)))

    return proxies


class Retrieve:

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.proxies = proxies_file()

    def retrieve_picture(self, pictureid):

        link = 'https://www.instagram.com/p/{}/'.format(pictureid)

        proxy, useragent = random.choice(self.proxies)

        resp = requests.get(link, headers={"User-Agent": useragent}, proxies={"https": proxy}, timeout=60)

        resp_json = re.findall(r'(?<=window\._sharedData = )(?P<json>.*)(?=;</script>)', resp.text)

        with open('./downloads/json/{}.json'.format(pictureid), 'w') as f:
            f.write(datetime.now().strftime('%s') + '\n')
            f.writelines(resp_json)

        #retrieved_at_time

        imagelink = re.findall(r'"display_url":"([^"]+)"', resp.text)

        imagefile = requests.get(imagelink[0], stream=True)

        with open('./downloads/picture/{}.jpg'.format(pictureid), 'wb') as f:
            shutil.copyfileobj(imagefile.raw, f)

