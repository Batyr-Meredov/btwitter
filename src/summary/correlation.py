import os

import numpy as np
import pandas as pd
from lets_plot import *
from scipy import stats
from datetime import date
import statsmodels.api as sm

from src.summary.confirmed import ConfirmedCases
from src.summary.deaths import DeathCases

LetsPlot.setup_html()


# https://jupyter.org/install

class Correlation:
    def __init__(self):
        self.shift = 5
        self.period = ''
        self.moving_average = 3
        self.ggsize_width = 1800
        self.ggsize_height = 1000
        self.batyr_base_dir = f'{os.path.dirname(os.path.dirname(__file__))}/../batyr'
        self.data_base_dir = f'{os.path.dirname(os.path.dirname(__file__))}/../results'
        self.filename = f'{self.data_base_dir}/14122021-14012022-usa.csv'
        self.sent_by_day_url = 'https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset/main/Summary_Sentiment/Sent_by_day.csv'

    def get_sentiment(self):
        df = pd.read_csv(
            self.filename,
            encoding="ISO-8859-1",
            na_filter=False,
            names=['sentiment', 'date', 'count'],
            dtype={'sentiment': str, 'date': str, 'count': int},
            usecols=[0, 1, 2],
            parse_dates=['date'],
        )
        df = pd.merge(
            df[df.sentiment == 'neutral']
            .rename({'count': 'neutral'}, axis=1)
            .drop(columns=['sentiment']),
            pd.merge(
                df[df.sentiment == 'positive']
                .rename({'count': 'positive'}, axis=1)
                .drop(columns=['sentiment']),
                df[df.sentiment == 'negative']
                .rename({'count': 'negative'}, axis=1)
                .drop(columns=['sentiment']),
                on='date', how="inner"),
            on='date', how="inner")
        return df

    def get_sentiment_by_day(self):
        df = pd.read_csv(
            self.sent_by_day_url,
            header=0,
            encoding="ISO-8859-1",
            na_filter=False,
            names=['year', 'month', 'day', 'sentiment', 'count', 'percent'],
            dtype={'year': int, 'month': int, 'day': int, 'sentiment': str, 'count': int, 'percent': float},
            usecols=[1, 2, 3, 4, 5, 6],
        )
        df['date'] = df.apply(lambda row: date(row['year'], row['month'], row['day']), axis=1)
        df = df.drop(columns=['year', 'month', 'day'])
        df = pd.merge(
            df[df.sentiment == 'neutral']
            .rename({'count': 'neutral'}, axis=1)
            .rename({'percent': 'neu'})
            .drop(columns=['sentiment', 'percent']),
            pd.merge(
                df[df.sentiment == 'positive']
                .rename({'count': 'positive'}, axis=1)
                .rename({'percent': 'pos'})
                .drop(columns=['sentiment', 'percent']),
                df[df.sentiment == 'negative']
                .rename({'count': 'negative'}, axis=1)
                .rename({'percent': 'neg'})
                .drop(columns=['sentiment', 'percent']),
                on='date', how="inner"),
            on='date', how="inner")
        return df

    def get_merged_sentiment(self):
        df = pd.read_csv(
            self.filename,
            encoding="ISO-8859-1",
            na_filter=False,
            names=['sentiment', 'date', 'count'],
            dtype={'sentiment': str, 'date': str, 'count': int},
            usecols=[0, 1, 2],
            parse_dates=['date'],
        )
        df = pd.merge(
            df[df.sentiment == 'neutral'].rename({'count': 'neutral'}, axis=1).drop(columns=['sentiment']),
            pd.merge(
                df[df.sentiment == 'positive'].rename({'count': 'positive'}, axis=1).drop(columns=['sentiment']),
                df[df.sentiment == 'negative'].rename({'count': 'negative'}, axis=1).drop(columns=['sentiment']),
                on='date', how="inner"),
            on='date', how="inner")
        return df

    def sentiment_negative_with_deaths_cases(self, ds):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        # ds['mTweet'] = ds['nTweet'].rolling(self.moving_average).mean()
        # ds['pmTweet'] = ds['pTweet'].rolling(self.moving_average).mean()
        # ds['Discounted_Price'] = ds['negative'] - (0.1 * df['Cost'])

        # dsm = ds.rolling(3, min_periods=None, center=False, win_type=None, on=None, axis=0).mean()
        # ds['SMA30'] = ds['nTweet'].rolling(3).mean()

        # dc = DeathCases().get_time_series_covid19_death_for_us()
        dc = ConfirmedCases().get_time_series_covid19_confirmed_for_us()
        print(dc)

        dcc = dc.copy()
        dcc = dcc.drop(columns=['year', 'month', 'day'])

        dcc['daily_confirmed_cases_shift'] = dcc['daily_confirmed_cases'].shift(self.shift)
        #dcc['mDDC'] = dcc['ddc'].rolling(self.moving_average).mean()

        ds.date = ds.date.astype('datetime64[ns]')
        dcc.date = dcc.date.astype('datetime64[ns]')
        merged = pd.merge(dcc, ds, on='date', how="inner")
        #merged = merged[merged['date'] >= '2021-08-01']
        #merged = merged[merged['date'] <= '2021-09-30']
        # print(f"sentiment: {len(ds)}, covid: {len(dc)}, merged: {len(mergwed)}")
        copied = merged[['date', 'daily_confirmed_cases', 'negative', 'positive', 'neutral']].copy()
        print(copied)
        print(merged)
        return merged

    def plot_correlation(self, dfl, _x, _y):
        __plot = ggplot(dfl, aes(x=_x, y=_y)) + \
                 geom_point(aes(color=_y, fill=_y), shape=21, size=3, alpha=.2) + \
                 scale_color_gradient(low='#abd9e9', high='#d7191c') + \
                 scale_fill_gradient(low='#abd9e9', high='#d7191c') + \
                 ggtitle(f'Relation Between {_x} and {_y}') + \
                 ggsize(self.ggsize_width, self.ggsize_height) + \
                 theme_classic() + theme(legend_position='right')

        filename = f'correlation_{_x}-{_y}-{self.period}.html'
        ggsave(__plot, filename, path='../../docs/images', iframe=False)
        __plot.show()

    def calculate_and_plot(self, x, y):
        ds = self.get_sentiment()
        df = self.sentiment_negative_with_deaths_cases(ds).copy()
        pr = stats.pearsonr(df[x], df[y])
        ccf = sm.tsa.stattools.ccf(df[x], df[y])
        print(ccf)
        self.plot_correlation(df, x, y)

    def plot_two(self):
        ds = self.get_sentiment_by_day()
        df = self.sentiment_negative_with_deaths_cases(ds).copy()

        p1 = ggplot(df, aes('date', fill='pmTweet'))
        p1 + geom_point(aes(y='pmTweet', color='pmTweet'), size=3, alpha=0.8) + \
        ggtitle('Scatter plot')
        # p1 + geom_point(aes(y='month', color=as_discrete("year")), size=3, alpha=0.8)
        # p1 + ggtitle('Scatter plot')
        #        geom_line(aes(group="year", color=as_discrete("year")), size=1, \
        #                  tooltips=layer_tooltips().title("@year") \
        #                  .format("@{mean temperature}", ".2f") \
        #                  .line("@|@{mean temperature}") \
        #                  .line("date|@month/@day/@year")) + \
        #        scale_x_continuous(breaks=list(range(1, 32))) + \
        #        facet_grid(y="month", scales='free') + \
        #        ylab("month") + \
        #        ggtitle("Mean Temperature for Each Month") + \
        #        ggsize(self.ggsize_width, self.ggsize_height) + \
        #        theme(legend_position='bottom')
        p1.show()

    def example(self, _x, _y):
        ds = self.get_sentiment_by_day()
        df = self.sentiment_negative_with_deaths_cases(ds).copy()
        p = ggplot(df, aes(x=_x, fill=_y)) + \
            geom_point(aes(y=_y, color=_y), size=3, alpha=0.8) + \
            ggtitle('Scatter plot') + theme(legend_position='none') + \
            geom_histogram()
        #            + geom_bar(alpha=0.5)
        p.show()

    def e1(self):
        # define data
        df = self.get_sentiment().copy()
        p1 = ggplot() + \
             geom_area(aes(x='date', y='nTweet', group='sentiment', color='sentiment', fill='sentiment'), data=df,
                       alpha=.2, \
                       tooltips=layer_tooltips().line('@sentiment') \
                       .format('@nTweet', '.0f') \
                       .line('Datum|@date') \
                       .line('Anzahl Tweets|@nTweet')) + \
             scale_x_datetime() + \
             scale_x_datetime() + \
             ylab('nTweet') + theme(legend_position='none') + ggsize(self.ggsize_width, self.ggsize_height)

        ggsave(p1, 'sentiment.html', path='../../docs/images', iframe=False)
        p1.show()

    def plot_line(self):
        # ds = self.get_merged_sentiment()
        ds = self.get_sentiment_by_day()
        df = self.sentiment_negative_with_deaths_cases(ds)
        # p = ggplot(df, aes(x='date', y='negative')) + geom_histogram()
        # geom_histogram(aes(x='mean_temp', group='year', color='year', fill='year'), \

        tooltips_ddc = layer_tooltips().line('Datum|@date').format('@date', '%d.%m.%Y').line('Todesfälle|@ddc')
        tooltips = lambda sent: layer_tooltips().line('Datum|@date').line(f'Anzahl {sent} Tweets|@{sent}')

        p1 = ggplot(df, aes(x='date', y='ddc')) + geom_line(tooltips=tooltips_ddc)
        p2 = ggplot(df, aes(x='date', y=f'negative')) + geom_line(tooltips=tooltips('negative'))
        p3 = ggplot(df, aes(x='date', y=f'positive')) + geom_line(tooltips=tooltips('positive'))
        p4 = ggplot(df, aes(x='date', y=f'neutral')) + geom_line(tooltips=tooltips('neutral'))

        ggplot(df, aes(x='date', y='ddc')) + \
        ggsize(7000, 2000) + \
        geom_histogram(aes(color='ddc', fill='ddc'), data=df)

        # bunch.add_plot(p1, 0, 0, 600, 300)
        # bunch.add_plot(p2, 0, 300, 600, 200)

        bunch = GGBunch()
        bunch.add_plot(p1, 0, 0, 2000, 300)
        bunch.add_plot(p2, 0, 300, 2000, 300)
        bunch.add_plot(p3, 0, 600, 2000, 300)
        bunch.add_plot(p4, 0, 900, 2000, 300)
        bunch.show()
        ggsave(bunch, 'all.html', path='../../docs/images', iframe=False)


if __name__ == '__main__':
    # Correlation().plot_line()
    # Correlation().example('date', 'mDDC')
    corr = Correlation()
    corr.period = '14122021-14012022'
    corr.calculate_and_plot(x='daily_confirmed_cases_shift', y='positive')
