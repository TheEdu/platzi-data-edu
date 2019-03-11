import argparse
import logging
import re
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

import news_page_objects as news
from common import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ws")

# Regular expresion definitions
is_well_former_link = re.compile(r'^https?://.+/.+$')
is_root_path = re.compile(r'^/.+$')


def _build_link(host, link):
    if is_well_former_link.match(link):
        return link
    elif is_root_path.match(link):
        return '{}{}'.format(host, link)
    else:
        return '{host}/{uri}'.format(host=host, uri=link)


def _fetch_links(news_site_uid, host):
    logger.info('Start fetching links at {}'.format(news_site_uid))
    links = []
    try:
        homepage = news.HomePage(news_site_uid, host)
        links = homepage.article_links
    except Exception as e:
        logger.error('ERROR fetching links: {}'.format(e), exc_info=False)
    return links


def _fetch_article(news_site_uid, host, link):
    logger.info('Start fetching article at {}'.format(link))
    article = None
    try:
        article = news.ArticlePage(news_site_uid, _build_link(host, link))
    except (HTTPError, MaxRetryError) as e:
        logger.warning('ERROR fetching article: {}'.format(e), exc_info=False)
    except Exception as e:
        logger.error('ERROR fetching article: {}'.format(e), exc_info=False)

    if article and not article.body:
        logger.warning('Invalid article. There is no body')
        return None

    return article


def _news_scraper(news_site_uid):
    host = config()['news_sites'][news_site_uid]['url']
    logger.info('Beginning scraper for {}'.format(host))

    links = _fetch_links(news_site_uid, host)
    articles = []
    for link in links:
        article = _fetch_article(news_site_uid, host, link)

        if article:
            logger.info('Article fetched: {}'.format(article.title))
            articles.append(article)
    print('Total de Links Encontrados: {}'.format(len(links)))
    print('Total de Articulos Encontrados: {}'.format(len(articles)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    news_site_choices = list(config()['news_sites'].keys())

    parser.add_argument('news_site',
                        help='The news site that you want to scrape',
                        type=str,
                        choices=news_site_choices)

    args = parser.parse_args()
    _news_scraper(args.news_site)
