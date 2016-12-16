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

    def __init__(self, ids, urls, dates, site, config=None, **kwargs):

        self.ids = ids
        self.urls = urls
        self.dates = dates
        self.site = site
        self.config = config or Configuration()

    def create_article(self, id, url, date):
        article = Article(url, fetch_images = False)        
        article.id = id
        article.date = date
        return article

    def generate_articles(self):
        self.articles = [self.create_article(id, url, date) 
                         for id, url, date in
                         zip(self.ids, self.urls, self.dates)]

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

    def __init__(self, sqldb, sites=None, site_query=None):
        self.sql_engine = create_engine(sqldb)
        self.sites = sites or self.get_sites(site_query)

    def get_sites(self, query):
        """ Determine acceptable base_urls to collect using query """
        if not query:
            query = """
                    SELECT base_url, page_id FROM fb_posts
                    GROUP BY base_url, page_id
                    """

        return pd.read_sql(query, self.sql_engine)

    def collect_url_data(self, retrieved_btw=None, chunksize=None):
        """
        Create a dataframe generator, each with `chunksize` rows,
        which has equal numbers of articles from each source

        Get subsets of the data s.t. we have ArticleSets of equal sizes
        when running news_pool on multiple sources.
        """

        if not retrieved_btw:
            retrieved_btw = {'start':'2000-01-01', 'end': '2024-01-01'}
        
        query = """
                SELECT post_id, base_url, page_id, link, created_time 
                FROM (SELECT post_id, base_url, page_id, link, created_time, 
                             ROW_NUMBER() OVER (PARTITION BY base_url 
                                                ORDER BY created_time DESC) AS rownum
                      FROM fb_posts fb
                      WHERE date(retrieved_on) >= '{start}' and
                            date(retrieved_on) <= '{end}' and
                            page_id in ('{page_ids}') and 
                            base_url in ('{base_urls}') and
                            NOT EXISTS (SELECT 1 FROM articles a 
                                        WHERE fb.post_id = a.post_id)
                     ) AS article_set
                ORDER BY rownum, page_id
                """.format(page_ids="', '".join(self.sites.page_id),
                           base_urls="', '".join(self.sites.base_url),
                           **retrieved_btw)

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
            site_url = self.sites.base_url[self.sites.page_id == page_id].tolist()
            is_site_url = page_df.base_url.isin(site_url)

            articleset = ArticleSet(ids=page_df.post_id[is_site_url],
                                    urls=page_df.link[is_site_url],
                                    dates=page_df.created_time[is_site_url],
                                    site=site_url)

            articleset.generate_articles()
            asets.append(articleset)

        report_progress(('collecting {} articles from {} to {}...').format(
                         len(df), begin_date, end_date))

        return asets

    def save_articles(self, articlesets):
        """ Read articles from a source and save them to a sql server """

        cur_time = time.ctime()

        for articleset in articlesets:
            site = articleset.site
            article_df = pd.DataFrame.from_records(
                                 [{'post_id': a.id,
                                   'url': a.url,
                                   'base_url': ', '.join(site),
                                   'title': a.title,
                                   'authors': ', '.join(a.authors),
                                   'article_text': a.text,
                                   'date': a.date,
                                   'retrieved_on': cur_time}
                                    for a in articleset.articles if a.text])

            if not article_df.empty:
                article_df.to_sql('articles', self.sql_engine,
                                  if_exists='append', index=None)

    def get_articles(self, chunksize=None, threads_per_source=1,
                     retrieved_btw=None):
        """ Download articles from multiple sources in parallel """

        site_url_data = self.collect_url_data(retrieved_btw=retrieved_btw,
                                              chunksize=chunksize)

        for df in site_url_data:
            articlesets = self.build_articlesets(df)
            news_pool.set(articlesets, threads_per_source=threads_per_source)
            news_pool.join()
            self.save_articles(articlesets)

        report_progress('\ndone!')
