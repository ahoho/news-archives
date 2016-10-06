import time

import pandas as pd
from newspaper import news_pool, Article, Source
from newspaper.configuration import Configuration
from sqlalchemy import create_engine
from . import report_progress

class ArticleSet(Source):
    """ 
    Rather than scraping articles from a site, use a known list of
    urls. Ducktyped to work with news_pool
    """

    def __init__(self, article_urls, article_dates, pub_id, config=None, **kwargs):
        self.config = config or Configuration()
        self.article_urls = article_urls
        self.article_dates = article_dates
        self.pub_id = pub_id

    def create_article(self, url, date):
        article = Article(url, fetch_images = False)
        article.date = date
        return article

    def generate_articles(self):
        self.articles = [self.create_article(url, date) for url, date in
                         zip(self.article_urls, self.article_dates)]

    def download_articles(self, threads=1):
        """ Override superclass method to download AND parse articles """
        super(ArticleSet, self).download_articles(threads=threads)

        for article in self.articles:
            article.parse()

class NewsArchiver(object):
    """ 
    Collection of ArticleSets built using data from SQL db,
    iterates through each article to collect text.
    """

    def __init__(self, sqldb, publications=None):
        self.sql_engine = create_engine(sqldb)
        self.publications = publications or self.collect_publications()

    def collect_publications(self):
        """ 
        Get the distinct publications in the database and their
        most associated urls (so as to only keep self-published articles
        """
        query = """SELECT page_id, page_name, base_url, count(*) as num_posts
                   FROM fb_posts
                   GROUP BY page_id, page_name, base_url"""

        pub_data = pd.read_sql(query, self.sql_engine)\
                     .replace('^www\\.', '', regex=True)\
                     .sort_values(['page_id', 'num_posts'], ascending=False)\
                     .groupby('page_id')\
                     .first()\
                     .to_dict()['base_url']

        return pub_data

    def collect_url_data(self, retrieved_btw=None, chunksize=None):
        """
        Create a dataframe generator, each with `chunksize` rows,
        which has equal numbers of articles from each source

        Gett subsets of the data s.t. we have ArticleSets of equal sizes
        when running news_pool on multiple threads.

        The `GROUP BY` ensures no duplicates.

        The below query is postgres specific, for other dialects,
        a simple `ORDER BY created_time` can stand in for the `PARTITION`
        """

        if not retrieved_btw:
            retrieved_btw = {'start':'2000-01-01', 'end': '2020-01-01'}
        
        query = """
                SELECT page_id, base_url, link, created_time 
                FROM (SELECT page_id, base_url, link,
                             min(created_time) as created_time, ROW_NUMBER() 
                      OVER (PARTITION BY page_id 
                            ORDER BY min(created_time) DESC) AS Row_ID
                      FROM fb_posts
                      WHERE retrieved_on >= '{start}' and
                            retrieved_on <= '{end}'
                      GROUP BY page_id, base_url, link) AS A
                ORDER BY Row_ID, page_id
                """.format(**retrieved_btw)

        df = pd.read_sql(query, self.sql_engine, chunksize=chunksize)

        if not chunksize:
            df = [df]

        return df

    def build_articlesets(self, df):
        """ Build articlesets from dataframe of urls and record progress """
        begin_date = min(df['created_time'])
        end_date = max(df['created_time'])
        
        asets = []
        for page_id, page_df in df.groupby('page_id'):
            is_site_url = page_df.base_url.str.contains(
                                                   self.publications[page_id],
                                                   na = False)
            articleset = ArticleSet(page_df.link[is_site_url],
                                    page_df.created_time[is_site_url],
                                    page_id)

            articleset.generate_articles()
            asets.append(articleset)

        report_progress(('collecting {} articles from {} to {}...').format(
                         len(df), begin_date, end_date))

        return asets

    def save_articles(self, articlesets):
        """ Read articles from a source and save them to a sql server """
        # TODO: consider adding pk/fk to our tables
        cur_time = time.ctime()

        for articleset in articlesets:
            page_id = articleset.pub_id
            article_df = pd.DataFrame.from_records(
                                 [{'url': a.url,
                                   'page_id': page_id,
                                   'page_base_url': self.publications[page_id],
                                   'title': a.title,
                                   'authors': ', '.join(a.authors),
                                   'text': a.text,
                                   'date': a.date,
                                   'retrieved_on': cur_time}
                                    for a in articleset.articles if a.text])

            if not article_df.empty:
                article_df.to_sql('articles', self.sql_engine,
                                  if_exists='append', index=None)

    def get_articles(self, chunksize=None, threads_per_source=1):
        """ Download articles from multiple sources in parallel """

        pub_url_data = self.collect_url_data(chunksize=chunksize)

        for df in pub_url_data:
            articlesets = self.build_articlesets(df)
            news_pool.set(articlesets, threads_per_source=threads_per_source)
            news_pool.join()
            self.save_articles(articlesets)

        report_progress('\ndone!')
