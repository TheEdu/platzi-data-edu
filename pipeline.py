import subprocess

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
news_sites_uids = ['impactolocal', 'lavozzarate']


def _extract():
    logger.info('Starting Extract Process...')
    subprocess.run(['python', 'main.py', '-l'] + news_sites_uids, cwd='./extract-async')
    for news_site_uid in news_sites_uids:
        subprocess.run(['find', '.', '-name', '{}*'.format(news_site_uid),
                        '-exec', 'mv', '{}', '../transform/{}_.csv'.format(news_site_uid), ';'],
                        cwd='./extract-async')


def _transform():
    logger.info('Starting transform process...')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = '{}_.csv'.format(news_site_uid)
        clean_data_filename = 'clean_{}'.format(dirty_data_filename)
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform')
        subprocess.run(['rm', dirty_data_filename], cwd='./transform')
        subprocess.run(['mv', clean_data_filename, '../load/{}.csv'.format(news_site_uid)], cwd='./transform')


def _load():
    logger.info('Starting load process...')
    for news_site_uid in news_sites_uids:
        clean_data_filename = '{}.csv'.format(news_site_uid)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
        subprocess.run(['rm', clean_data_filename], cwd='./load')


def _main():
    _extract()
    _transform()
    _load()


if __name__ == '__main__':
    _main()
