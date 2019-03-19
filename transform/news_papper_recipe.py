import argparse
from urllib.parse import urlparse
import pandas as pd
import hashlib
import nltk
import re
from nltk.corpus import stopwords

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


def _tokenize_column(df, column_name, stop_words):
    return (df
            .dropna()
            .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
            .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
            .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
            .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
            .apply(lambda valid_word_list: len(valid_word_list))
            )


def _tokenize_columns(df, columns, stop_words):
    _log('Tokenazing columns')
    for column in columns:
        df['n_token_' + column] = _tokenize_column(df, column, stop_words)
    return df


def _remove_duplicate_entries(df, column_name):
    _log('Removing Duplicate Entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df


def _drop_rows_with_missing_values(df):
    _log('Dropping rows with missing values')
    return df.dropna()


def _remove_system_path(filename):
    return re.search(r"[^\\/]+$", filename).group()


def _save_data(df, filename):
    clean_filename = 'clean_{}'.format(_remove_system_path(filename))
    _log('Saving data at location: {}'.format(clean_filename))
    df.to_csv(clean_filename, encoding='utf-8-sig')


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

    stop_words = set(stopwords.words('spanish'))
    df = _tokenize_columns(df, ['title_csv', 'body_csv'], stop_words)
    df = _remove_duplicate_entries(df, 'title_csv')
    df = _drop_rows_with_missing_values(df)
    _save_data(df, filename)

    print(df[['title_csv', 'n_token_title_csv', 'n_token_body_csv']])
    return df


if __name__ == '__main__':
    args = _get_args('filename', 'The parth to the dirty data', str)
    main(args.filename)
