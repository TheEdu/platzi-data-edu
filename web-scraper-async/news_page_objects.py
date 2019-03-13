import bs4
import re

from common import config

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


class NewsPage:
    def __init__(self, news_site_uid):
        self._config = config()['news_sites'][news_site_uid]
        self._queries = self._config['queries']
        self._url = self._config['url']
        self._html = None

    def _select(self, query_string):
        return self._html.select(query_string)

    def _select_list(self, query_string_list):
        results = []
        for query_string in query_string_list:
            results = results + self._html.select(query_string)
        return results

    async def visit(self, session):
        async with session.get(self._url) as response:
            text = await response.text()
            self._html = bs4.BeautifulSoup(text, 'html.parser')


class HomePage(NewsPage):
    def __init__(self, news_site_uid):
        super().__init__(news_site_uid)

    @property
    def article_links(self):
        link_list = []
        for link in self._select_list(self._queries['homepage_article_links']):
            if link and link.has_attr('href'):
                link_list.append(link)
        return set(link['href'] for link in link_list)


class ArticlePage(NewsPage):
    def __init__(self, news_site_uid, article_url):
        super().__init__(news_site_uid)
        self._url = _build_link(self._url, article_url)

    @property
    def body_csv(self):
        results = self._select(self._queries['article_body'])
        text = ''
        for result in results:
            text += result.text
        return text

    @property
    def title_csv(self):
        result = self._select(self._queries['article_title'])
        return result[0].text if len(result) else ''
