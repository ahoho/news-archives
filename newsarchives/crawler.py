import requests
import time
import logging
from collections import defaultdict
from urllib.parse import parse_qs, urlparse

import facebook as fb
import pandas as pd
from sqlalchemy import create_engine
from . import report_progress

class FBGraphCrawler(object):
    """ Object that stores a list of facebook pages and
        retrieves posts from their feeds """

    def __init__(self, access_token, sqldb, pages):
        # although this depends on python-sdk, the facebook
        # graph API is simple enough to use that, should this dependency cause
        # problems, requests alone is sufficient
        self.graph = fb.GraphAPI(access_token)
        self.pages = self.test_pages(pages)
        self.sql_engine = create_engine(sqldb)
        self.errors = {page: [] for page in list(self.pages.values())}

    def test_pages(self, page_names):
        """ Make sure each page exits and get its static facebook id """
        page_ids = defaultdict(str)

        for page in page_names:
            try:
                page_ids[page] = self.graph.get_connections(page, '')\
                                           .get('id')

            except fb.GraphAPIError:
                logging.warning(
                    'Page for {} not found, removing from list'.format(page))

        return page_ids

    def save_all_page_feeds(self, through_date=None):
        """ For each page, collect list of urls """

        for page_name, page_id in list(self.pages.items()):
            report_progress('\nscraping page {}...\n'.format(page_name))

            # collect post data from pages
            post_data = [post for post in
                         self.collect_feed_posts(page_id, through_date)]

            report_progress('\nsaving to sql...')

            # save to sql
            post_df = pd.DataFrame.from_records(post_data) \
                                  .assign(retrieved_on=time.ctime(),
                                          page_name=page_name,
                                          page_id=page_id) \
                                  .drop_duplicates() \
                                  .to_sql('fb_posts_20161012', self.sql_engine,
                                          if_exists='append', index=False)

    def collect_feed_posts(self, page_id, through_date=None, error_limit=30):
        """
        Iterate through the external URLs of all posts from a given page,
        continuing through the feed history until `through_date`
        """

        params = {'fields': 'id,link,shares,created_time,type',
                  'limit': '100'}
        self.errors['consecutive'] = 0

        # continue unless consecutive errors exceed limit
        while self.errors['consecutive'] < error_limit:

            try:
                response = self.graph.get_connections(
                    page_id, 'posts', **params)
                # reset consecutive error count if no error occurs
                self.log_error(page_id, reset=True)
            except fb.GraphAPIError as error:
                self.log_error(page_id, error)
                continue

            # collect results
            for post in response['data']:
                # want to make sure we only collect external links
                if post.get('type') == 'link':
                    post_url = self.unshorten_url(post.get('link'), page_id)

                    parsed_post = {
                        'post_id': post.get('id'),
                        'link': post_url,
                        'base_url': self.get_base_url(post_url),
                        'shares': post.get('shares', {}).get('count'),
                        'created_time': post.get('created_time')}

                    report_progress('on post from {}'.format(
                        parsed_post['created_time']))

                    yield parsed_post

            next_page = response.get('paging', {}).get('next')

            # continue through pages until `through_date` is hit
            # or no more pages exist
            if next_page and parsed_post['created_time'] >= through_date:
                # update params for next page
                params = parse_qs(urlparse(next_page).query)
            else:
                report_progress('\ncompleted without excess errors')
                return

    def unshorten_url(self, url, page_id):
        """ Turn a shortened URL into a resolved, static URL for a story """
        if url:
            try:
                parsed_url = requests.head(url, allow_redirects=True).url
                self.log_error(page_id, reset=True)
                return parsed_url

            except (requests.ConnectionError, requests.TooManyRedirects,
                    UnicodeError) as error:
                self.log_error(page_id, error)
                return None

    def get_base_url(self, url):
        if url:
            return urlparse(url).netloc

    def get_page_url(self, page):
        """ Get websites associated with each FB page """
        response = self.graph.get(page, params={'fields': 'website'})
        return response.get('website')

    def log_error(self, page_id, error=None, reset=False):
        """
        If either the facebook API or the urls yield too many
        consecutive errors, stop querying
        """
        if error:
            self.errors[page_id] += [error]
            self.errors['consecutive'] += 1
        if reset:
            self.errors['consecutive'] = 0