import argparse
import pandas as pd

from article import Article
from base import Base, Session, engine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _main(filename):
    Base.metadata.create_all(engine)
    session = Session()
    articles = pd.read_csv(filename)

    for index, row in articles.iterrows():
        logger.info('Loading article uid {} into DB'.format(row['uid']))
        article = Article(row['uid'],
                          row['body_csv'],
                          row['host'],
                          row['newspapper_uid'],
                          row['n_token_body_csv'],
                          row['n_token_title_csv'],
                          row['title_csv'],
                          row['url_csv'])
        session.add(article)
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)
    args = parser.parse_args()
    _main(args.filename)
