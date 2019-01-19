"""
This module contains functions and classes related to retrieving data from Instagram.
Basically, it's a specialized wrapper for aiohttp.

How retrieved data should be processed and stored afterwards is out of scope of this module.
"""
import asyncio
import itertools
import json
import logging
import re

import aiohttp
from aiohttp.client_exceptions import ClientResponseError, ClientConnectionError, ServerTimeoutError

from .utils import read_lines, measure_time


log = logging.getLogger(__name__)


class FetchError(Exception):
    """This exception is raised when resource can't be fetched from Instagram."""


class PageNotFound(Exception):
    """This exception is raised when Instagram returns 404."""


class Session:
    """
    Session to make HTTP requests asynchronously. Encapsulates all HTTP requests to Instagram.

    Usage:

        async with Session(...) as session:
            await session.get_something()
    """

    def __init__(self, use_proxy=True):
        user_agents = read_lines('./config/agentstrings.conf')
        servers = read_lines('./config/proxys.conf')
        self._proxies = itertools.cycle(list(zip(user_agents, servers)))
        self._use_proxy = use_proxy
        self._client = None

    async def __aenter__(self):
        self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), raise_for_status=True)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.close()
            self._client = None

    async def fetch(self, url, binary=False):
        """
        Fetch URL and return response (as text or raw bytes).

        :param binary: If True, return raw bytes.
        """
        delay = 1
        while True:
            kw = {}
            if self._use_proxy:
                proxy, useragent = next(self._proxies)
                kw['proxy'] = f'http://{proxy}'
                kw['headers'] = {
                    'User-Agent': useragent
                }
            else:
                proxy = 'no_proxy'
            try:
                async with self._client.get(url, **kw) as response:
                    if binary:
                        return await response.read()
                    else:
                        return await response.text()
            except ClientResponseError as ex:
                if ex.status == 404:
                    log.info(f'HTTP 404 Not Found ({proxy}): {url}')
                    raise PageNotFound
                elif ex.status == 429:
                    log.info(f'HTTP 429 Rate Limit ({proxy}): {url}')
                    delay *= 2
                else:
                    # Raise all other HTTP errors for now to see what action is needed
                    raise
            except ServerTimeoutError as ex:
                log.info(f'Timeout Error ({proxy}): {url}\n{ex}')
            except ClientConnectionError as ex:
                log.info(f'Connection Error ({proxy}): {url}\n{ex}')

            # Delay between attempts
            await asyncio.sleep(delay)


    async def fetch_json(self, url):
        """
        Fetch JSON from Instagram. Return deserialized JSON (Python dict).

        :param url: Instagram URL, e.g. 'https://www.instagram.com/president'.
        :return: Deserialized JSON.
        :raise: FetchError, PageNotFound
        """
        text = await self.fetch(url)

        # Return deserialized JSON
        try:
            json_text = self._JSON_RE.search(text).group(1)
        except AttributeError:
            raise FetchError("Unexpected response (can't extract JSON)")
        try:
            return json.loads(json_text)
        except ValueError:
            raise FetchError('Invalid JSON')

    # Regex to extract JSON from Instagram pages
    # (it's the same for users, posts and locations)
    _JSON_RE = re.compile(r'window\._sharedData\s*=\s*(\{.*?\});</script>', re.DOTALL)

    async def fetch_location(self, location_id):
        """
        Fetch location JSON from Instagram. Return deserialized JSON (Python dict).

        :param url: Instagram URL, e.g. 'https://www.instagram.com/explore/locations/761560033/'.
        :return: Deserialized JSON.
        :raise: FetchError, PageNotFound
        """
        with measure_time(location_id, 'Fetching JSON'):
            return await self.fetch_json(f'https://www.instagram.com/explore/locations/{location_id}/')

    async def fetch_user(self, user_id):
        with measure_time(user_id, 'Fetching JSON'):
            return await self.fetch_json(f'https://www.instagram.com/{user_id}/')

    async def fetch_post(self, post_id):
        with measure_time(post_id, 'Fetching JSON'):
            return await self.fetch_json(f'https://www.instagram.com/p/{post_id}/')

    async def fetch_image(self, post_data):
        try:
            url = post_data['entry_data']['PostPage'][0]['graphql']['shortcode_media']['display_url']
        except KeyError:
            raise FetchError('No image URL in JSON')
        return await self.fetch(url, binary=True)


# The code below is temporary, for testing
async def main():
    import sys
    method = sys.argv[1]
    id = sys.argv[2]
    async with Session(use_proxy=False) as session:
        if method == 'fetch_image':
            data = await session.fetch_post(id)
            image = await session.fetch_image(data)
            with open('output.jpg', 'wb') as file:
                file.write(image)
            print(f'Saved {len(image)} bytes to output.jpg')
        else:
            data = await getattr(session, method)(id)
            json.dump(data, sys.stdout, indent=2, ensure_ascii=True)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
