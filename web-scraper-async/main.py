import logging
import aiohttp
import asyncio
import csv
import datetime

import news_page_objects as news
from common import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ws")


async def _fetch_links(news_site_uid, session):
    error = 0
    links = []
    try:
        homepage = news.HomePage(news_site_uid)
        await homepage.visit(session)
        links = homepage.article_links
    except Exception as e:
        logger.error('ERROR fetching links: {}'.format(e), exc_info=False)
        error = 'ERROR fetching links: {}'.format(e)
    return (error, links, news_site_uid)


async def _fetch_article(news_site_uid, link, session):
    article = None
    error = 0
    logger.info('Start fetching article at {}'.format(link))
    try:
        article = news.ArticlePage(news_site_uid, link)
        await article.visit(session)

        if article and not article.body_csv:
            logger.warning('Invalid article. There is no body')
            error = 'Invalid article. There is no body'
            article = None

    except Exception as e:
        logger.error('ERROR fetching article: {}'.format(e), exc_info=False)
        error = 'ERROR fetching article {}: {}'.format(link, e)

    return (error, article, news_site_uid)


async def _news_scraper():
    news_site_choices = list(config()['news_sites'].keys())
    tasks = []

    logger.info('Beginning scraping')
    async with aiohttp.ClientSession() as session:

        logger.info('Fetching articles links...')
        for news_site_uid in news_site_choices:
            tasks.append(_fetch_links(news_site_uid, session))
        home_pages = await asyncio.gather(*tasks)

        logger.info('Fetching articles...')
        tasks = []
        for index in range(len(home_pages)):
            error = home_pages[index][0]
            links = home_pages[index][1]
            news_site_uid = home_pages[index][2]

            # Check Error
            if error == 0:
                print(news_site_uid)
            else:
                logger.error(error)

            for link in links:
                tasks.append(_fetch_article(news_site_uid, link, session))

        # Waiting for all Articles
        articles = await asyncio.gather(*tasks)
        # Filter Articles with Error Code 0
        articles = list(filter(lambda article: article[0] == 0, articles))

        csv_headers = list(filter(
            lambda property: property.endswith('_csv'),
            dir(articles[0][1])))
        now = datetime.datetime.now().strftime('%d_%m_%Y')

        for index in range(len(articles)):
            error = articles[index][0]
            article = articles[index][1]
            news_site_uid = articles[index][2]

        for home_page in home_pages:
            home_name = home_page[2]
            out_file_name = '{}_{}.csv'.format(home_name, now)
            home_articles = list(
                filter(lambda article: article[2] == home_name, articles)
            )

            with open(out_file_name, mode='w+') as file:
                writer = csv.writer(file)
                writer.writerow(csv_headers)

                for article in home_articles:
                    articlePage = article[1]
                    try:
                        r = [str(getattr(articlePage, h)) for h in csv_headers]
                        writer.writerow(r)
                    except Exception as e:
                        logger.error('ERROR writing article: {}'.format(e))
    logger.info('Finishing scraping')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_news_scraper())
