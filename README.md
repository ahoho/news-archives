newsarchiver: News Article Archival
-----------------------------------

newsarchiver combines the [Facebook Graph
API](https://developers.facebook.com/docs/graph-api) and the excellent
[newspaper](http://newspaper.readthedocs.io/en/latest/) package to archive a
media source's articles to a database. Note that, since `newspaper` requires
Python 3, so does this package.

This particular marriage of APIs is designed to address the general absence of
article archives for online media outlets. The Facebook Graph API allows us to
obtain historical article data by collecting urls from a given website's official
feed, and `newspaper` scrapes the resulting article's contents.

Obviously, this limits us to the subset of articles that are posted on
Facebook, but the feed collection and article archival processes have
deliberately been kept separate so that the latter accepts url lists derived
via other means (e.g., RSS feeds).

This package was created in the service of providing data for natural language
processing tasks, in particular my analyses of partisan media sources.

## Example
```python
from newsarchiver import FBGraphCrawler, NewsArchiver

app_id = 'my_app_id'
app_secret = 'my_secret_key'
access_token = '{}|{}'.format(app_id, app_secret)

sql_path = 'postgres://postgres:postgres@localhost/articles.db'

# use page names or ids
pages = ['breitbart', '80256732576', 'democracynow', 'upworthy']

# save feeds to database called 'fb_posts'
crawler = FBGraphCrawler(access_token, sql_path, pages)
crawler.save_all_page_feeds(through_date = '2016-10-05')

# read from 'fb_posts' table and save contents of articles to an 'articles'
# table 
archiver = NewsArchiver(sql_path)
archiver.get_articles(chunksize=250)
```

## Notes

As of now, the structure of this package is relatively fixed, but I intend to
render it extensible in the future by creating base classes for
`FBGraphCrawler` and `NewsArchiver`.

On a similar note, there is a postgres-specific query in `collect_url_data()`
that allows `NewsArchiver` to get a roughly uniform distribution of sources
for each block of `chunksize` rows pulled from the Facebook feed table. This
particular optimization is  is not necessary and can be modified to accomodate
other SQL dialects. Note, however, that sqlite requires the use of multiple
databases if article urls are lazy-loaded (by way of a nonzero `chunksize`
argument).
