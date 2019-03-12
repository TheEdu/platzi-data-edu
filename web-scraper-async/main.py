import logging
import aiohttp
import asyncio

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
        error = 'ERROR fetching links: {}'.format(e)
    return (error, links, news_site_uid)


async def _fetch_article(news_site_uid, link, session):
    logger.info('Start fetching article at {}'.format(link))
    article = None
    try:
        article = news.ArticlePage(news_site_uid, link)
        await article.visit(session)
    except Exception as e:
        logger.error('ERROR fetching article: {}'.format(e), exc_info=False)

    if article and not article.body:
        logger.warning('Invalid article. There is no body')
        return None

    return article


async def _news_scraper():
    news_site_choices = list(config()['news_sites'].keys())
    tasks = []

    logger.info('Beginning scraper')
    async with aiohttp.ClientSession() as session:
        for news_site_uid in news_site_choices:
            tasks.append(_fetch_links(news_site_uid, session))
        home_pages = await asyncio.gather(*tasks)

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

        articles = await asyncio.gather(*tasks)
        for article in articles:
            print(article)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_news_scraper())
