import os
import time
import datetime
import pandas as pd
from os import getpid
from multiprocessing import Pool
from src.summary import utils
from src.summary.percona import Percona
from src.summary.scylla import ScyllaAdapter


class Indexing:
    def __init__(self):
        self.year_month = '2021_12'
        self.base_directory = '../../data/lopezbec'
        self.ner_directory = f'{self.base_directory}/ner/{self.year_month}'
        self.details_directory = f'{self.base_directory}/details/{self.year_month}'
        self.sentiment_directory = f'{self.base_directory}/sentiment/{self.year_month}'

    def get_all_files_from_directory(self, directory):
        print(f'get files for {self.year_month} from {directory}')
        return list(sorted(os.listdir(f'{self.base_directory}/{directory}/{self.year_month}')))

    def read_sentiment(self, filename):
        return pd.read_csv(
            f'{self.sentiment_directory}/{filename}',
            encoding="ISO-8859-1",
            header=0,
            na_filter=False,
            names=['tweet_id', 'sentiment'],
            dtype={'tweet_id': int, 'sentiment': str},
            usecols=[0, 1]
        )

    def read_details(self, filename):
        df = pd.read_csv(
            f'{self.details_directory}/{filename}',
            encoding="ISO-8859-1",
            header=0,
            na_filter=False,
            names=['tweet_id', 'country'],
            dtype={'tweet_id': int, 'country': str},
            usecols=[0, 6],
        )
        return df

    def read_ner(self, filename):
        return pd.read_csv(
            f'{self.ner_directory}/{filename}',
            encoding="ISO-8859-1",
            na_filter=False,
            header=0,
            names=['tweet_id', 'ner_text', 'start', 'end', 'ner_label', 'prob'],
            dtype={'tweet_id': int, 'start': int, 'end': int, 'ner_text': str, 'ner_label': str, 'prob': str},
            usecols=[0, 1, 2, 3, 4, 5]
        )

    def read_all_files(self, sentiment_filename):
        details_filename = sentiment_filename.replace('Sentiment', 'Details')

        details = self.read_details(details_filename)
        sentiment = self.read_sentiment(sentiment_filename)
        merged = pd.merge(sentiment, details, on='tweet_id', how='left')
        y, m, d, h = sentiment_filename.split('_')[0:4]
        created = datetime.datetime(year=int(y), month=int(m), day=int(d), hour=int(h))

        merged['created'] = created
        merged['ner_text'] = 'none'
        merged.fillna('none', inplace=True)
        # print(f'merged: {merged.shape[0]}\n{merged.iloc[:2]}')
        return merged

    def read_ner_and_details_files(self, ner_filename):
        details_filename = ner_filename.replace('NER', 'Details')

        ner = self.read_ner(ner_filename)
        details = self.read_details(details_filename)
        merged = pd.merge(ner, details, on='tweet_id', how='left')

        merged.fillna('none', inplace=True)
        # print(f'merged: {merged.shape[0]}\n{merged.iloc[:2]}')
        return merged

    def read_and_save_file(self, filename):
        try:
            y, m, d, h = filename.split('_')[0:4]
            created = datetime.datetime(year=int(y), month=int(m), day=int(d), hour=int(h))
            info = {"pid": getpid(), "filename": filename, "created": created}
            print(f'info: {info}')
            ScyllaAdapter().write_summary(self.read_all_files(filename), info)
        except Exception as e:
            print(f'ERROR: {e}')

    def read_and_save_ner_file(self, filename):
        print(f'read_and_save_file: {filename}')
        y, m, d, h = filename.split('_')[0:4]
        created = datetime.datetime(year=int(y), month=int(m), day=int(d), hour=int(h))
        info = {"pid": getpid(), "filename": filename, "created": created}
        df = self.read_ner_and_details_files(filename)
        Percona().write(df, info)

    def run(self, directory):
        start = time.time()
        filenames = self.get_all_files_from_directory(directory)
        filtered = list(filter(lambda x: '2021_12_13_' in x, filenames))
        print('FILTERED:', '\n'.join(filtered))

        with Pool(5) as pool:
            if directory == 'ner':
                pool.map(self.read_and_save_ner_file, filtered)
            else:
                pool.map(self.read_and_save_file, filtered)
        print(f'Parallel Run time: {time.time() - start}\n')

    def start(self, directory):
        try:
            self.run(directory)
        except Exception as e:
            print(e)
        exit()


if __name__ == '__main__':
    Indexing().start('sentiment')
