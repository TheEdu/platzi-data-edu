import argparse
from urllib.parse import urlparse
import pandas as pd

# Logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log(message, logger_type=None):
    if logger_type == 'info':
        return logger.info(message)
    else:
        return logger.info(message)


def get_args(name, help, type):
    parser = argparse.ArgumentParser()
    parser.add_argument(name,
                        help=help,
                        type=type)
    return parser.parse_args()


def _read_data(filename, encoding):
    log('Reading file {}'.format(filename))
    return pd.read_csv(filename, encoding, delimiter=',', engine='python')


def _extract_newspapper_uid(filename):
    log('Extracting newspapper uid')
    return filename.split('_')[0]


def _add_newspapper_uid_column(df, newspapper_uid):
    log('Filling newsppaper_uid colum with {}'.format(newspapper_uid))
    df['newspapper_uid'] = newspapper_uid
    return df


def _extract_host(df):
    log('Extracting host from urls')
    df['host'] = df['url_csv'].apply(lambda url: urlparse(url).netloc)
    return df


def main(filename):
    log('Starting cleaning process')
    df = _read_data(filename, 'ISO-8859-1')
    newspapper_uid = _extract_newspapper_uid(filename)
    df = _add_newspapper_uid_column(df, newspapper_uid)
    df = _extract_host(df)
    print(df)
    return df


if __name__ == '__main__':
    args = get_args('filename', 'The parth to the dirty data', str)
    main(args.filename)
