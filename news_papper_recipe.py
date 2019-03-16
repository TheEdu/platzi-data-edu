import argparse
from urllib.parse import urlparse
import pandas as pd
import hashlib

# Logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _log(message, logger_type=None):
    if logger_type == 'info':
        return logger.info(message)
    else:
        return logger.info(message)


def _get_args(name, help, type):
    parser = argparse.ArgumentParser()
    parser.add_argument(name,
                        help=help,
                        type=type)
    return parser.parse_args()


def _read_data(filename, encoding):
    _log('Reading file {}'.format(filename))
    return pd.read_csv(filename, encoding, delimiter=',', engine='python')


def _extract_newspapper_uid(filename):
    _log('Extracting newspapper uid')
    return filename.split('_')[0]


def _add_newspapper_uid_column(df, newspapper_uid):
    _log('Filling newsppaper_uid colum with {}'.format(newspapper_uid))
    df['newspapper_uid'] = newspapper_uid
    return df


def _extract_host(df):
    _log('Extracting host from urls')
    df['host'] = df['url_csv'].apply(lambda url: urlparse(url).netloc)
    return df


def _fill_missing_titles(df):
    _log('Filling missing titles')
    missing_title_mask = df['title_csv'].isna()
    missing_title = (df[missing_title_mask]['url_csv']
                     .str.extract(r'(?P<missin_titles>[^/]+)$')
                     .applymap(lambda title: title.split('-'))
                     .applymap(lambda title_word_list: ' '.join(title_word_list))
                     )
    df.loc[missing_title_mask, 'title_csv'] = missing_title.loc[:, 'missin_titles']
    return df


def _generate_uid_for_rows(df):
    _log('Generating UID for each row of the DataFrame')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url_csv'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    return df.set_index('uid')


def _text_replace(text, replacements):
    return "".join([replacements.get(char, char) for char in text])


def _remove_unwanted_chars(df, replacements):
    _log('Removing unwanted (replacements) chars')
    stripped_body = (df
                     .apply(lambda row: row['body_csv'], axis=1)
                     .apply(lambda body_text: _text_replace(body_text, replacements))
                     )
    df['body_csv'] = stripped_body
    return df


def main(filename):
    _log('Starting cleaning process')
    df = _read_data(filename, 'ISO-8859-1')
    newspapper_uid = _extract_newspapper_uid(filename)
    df = _add_newspapper_uid_column(df, newspapper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uid_for_rows(df)
    replacements = {
        "\n": " ",
        "\r": " ",
    }
    df = _remove_unwanted_chars(df, replacements)

    print(df[['body_csv', 'url_csv']])
    return df


if __name__ == '__main__':
    args = _get_args('filename', 'The parth to the dirty data', str)
    main(args.filename)
