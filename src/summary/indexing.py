import os
import time
import pandas as pd
from os import getpid
from multiprocessing import Pool
from src.summary.scylla import ScyllaAdapter


class Indexing:
    def __init__(self):
        self.year_month = 'lopezbec'
        self.base_directory = './data/lopezbec'
        self.ner_directory = f'{self.base_directory}/ner/{self.year_month}'
        self.details_directory = f'{self.base_directory}/details/{self.year_month}'
        self.sentiment_directory = f'{self.base_directory}/sentiment/{self.year_month}'

    def get_all_sentiment_files_from_directory(self):
        return list(sorted(os.listdir(self.sentiment_directory)))

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
        return pd.read_csv(
            f'{self.details_directory}/{filename}',
            encoding="ISO-8859-1",
            header=0,
            na_filter=False,
            names=['tweet_id', 'country', 'created'],
            dtype={'tweet_id': int, 'country': str, 'created': str},
            usecols=[0, 6, 7]
        )

    def read_ner(self, filename):
        return pd.read_csv(
            f'{self.ner_directory}/{filename}',
            encoding="ISO-8859-1",
            na_filter=False,
            header=0,
            names=['tweet_id', 'ner_text'],
            dtype={'tweet_id': int, 'ner_text': str},
            usecols=[0, 1]
        )

    def read_files(self, sentiment_filename):
        ner_filename = sentiment_filename.replace('Sentiment', 'NER')
        details_filename = sentiment_filename.replace('Sentiment', 'Details')

        details = self.read_details(details_filename)
        sentiment = self.read_sentiment(sentiment_filename)
        sentiment = sentiment[sentiment.sentiment == 'neutral']
        ner = self.read_ner(ner_filename).groupby('tweet_id').agg(lambda x: list(x))

        merged = pd.merge(sentiment, details, on='tweet_id', how='left')
        merged = pd.merge(merged, ner, on='tweet_id', how='left')

        merged.fillna('none', inplace=True)
        return merged

    def read_and_save_file(self, filename):
        ScyllaAdapter().write_summary(self.read_files(filename), f'pid: {getpid()}, filename: {filename}')

    def run(self):
        start = time.time()
        filenames = self.get_all_sentiment_files_from_directory()
        filtered = filenames[0:1]
        with Pool(5) as pool:
            pool.map(self.read_and_save_file, filtered)
        print(f'Parallel Run time: {time.time() - start}\n')

    def start(self):
        try:
            self.run()
        except Exception as e:
            print(e)
        exit()


if __name__ == '__main__':
    Indexing().start()
