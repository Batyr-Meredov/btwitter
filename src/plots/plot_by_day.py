# https://nbviewer.org/github/JetBrains/lets-plot-docs/blob/master/source/examples/demo/delhi_climate.ipynb
import pandas as pd
from lets_plot import *
from datetime import date
import scipy.stats as stats
from src.summary.deaths import DeathCases

LetsPlot.setup_html()


# %%
def fetch_sentiment_by_day():
    url = "https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset/main/Summary_Sentiment/Sent_by_day.csv"
    dfl = pd.read_csv(url, usecols=[1, 2, 3, 4, 5, 6])
    dfl['date'] = dfl.apply(lambda r: date(year=r.Year, month=r.Month, day=r.Day).strftime('%Y-%m-%d'), axis=1)
    return dfl


def get_filtered_sentiment_by_day(sentiment='negative', year="", month=""):
    df4 = fetch_sentiment_by_day()
    if sentiment:
        df4 = df4[df4.Sentiment == sentiment]
    if year:
        df4 = df4[df4.Year == year]
    if month:
        df4 = df4[df4.Month == month]
    df4 = df4.rename(columns={'Count': 'number_of_tweets'})
    return df4


def sentiment_negative_with_deaths_cases():
    sentiment_df = get_filtered_sentiment_by_day(sentiment='negative')
    covid_list = DeathCases().get_time_series_covid19_death_for_us()
    merged = pd.merge(sentiment_df, covid_list, on='date', how="inner")
    merged = merged.drop(columns=['Year', 'Month', 'Day', 'Sentiment', 'Avg_Per'])
    print(f"sentiment: {len(sentiment_df)}, covid: {len(covid_list)}, merged: {len(merged)}")
    return merged


def calculate_pearson_with_scipy(year='', month=''):
    dfl = sentiment_negative_with_deaths_cases().copy()
    if year:
        dfl = dfl[dfl.year == int(year)]
    if month:
        dfl = dfl[dfl.month == int(month)]
    print(dfl)
    return stats.pearsonr(dfl['daily_death_cases'], dfl['number_of_tweets'])


def plot_correlation(year='', month=''):
    dfl = sentiment_negative_with_deaths_cases().copy()
    if year:
        dfl = dfl[dfl.year == int(year)]
    if month:
        dfl = dfl[dfl.month == int(month)]
    plot = ggplot(dfl, aes("number_of_tweets", y="daily_death_cases")) + \
           geom_point(aes(color="number_of_tweets", fill="number_of_tweets"), shape=21, size=3, alpha=.2) + \
           scale_color_gradient(low='#abd9e9', high='#d7191c') + \
           scale_fill_gradient(low='#abd9e9', high='#d7191c') + \
           facet_grid(x='month', y='year') + \
           xlab('Monat') + ylab('Jahr') + \
           ggtitle("Relation Between 'number_of_tweets' and 'daily_death_cases'") + \
           ggsize(1800, 1000) + \
           theme_classic() + theme(legend_position='bottom')
    plot.show()


def calculate_and_plot(year='', month=''):
    r = calculate_pearson_with_scipy(year, month)
    print(r[0])
    print(r)
    plot_correlation(year, month)


# %%

calculate_and_plot('2021', '1')


# %%
def plot_deaths():
    df1 = DeathCases().get_time_series_covid19_death_for_us()
    p1 = ggplot() + \
         geom_histogram(aes(y='daily_death_cases', x='day', color='year', fill='year'),
                        stat='identity',
                        data=df1, bins=31, size=.5, alpha=.5,
                        tooltips=layer_tooltips()
                        .line('daily_death_cases|@daily_death_cases')
                        .line('day|@day')
                        .line('month|@month')
                        .line('year|@year')) + \
         scale_color_discrete() + scale_fill_discrete() + \
         facet_grid(x='month', y='year') + \
         xlab('Monat') + ylab('Jahr') + \
         ggtitle('Anzahl der "Todesfälle" nach Monat und Jahr') + \
         ggsize(800, 400) + \
         theme_classic() + theme(legend_position='bottom')
    return p1


def plot_positive():
    df1 = get_filtered_sentiment_by_day(sentiment='positive')
    p1 = ggplot() + \
         geom_histogram(aes(y='number_of_tweets', x='Day', color='Year', fill='Year'),
                        stat='identity',
                        data=df1, bins=31, size=.5, alpha=.5,
                        tooltips=layer_tooltips()
                        .line('number_of_tweets|@number_of_tweets')
                        .line('Day|@Day')
                        .line('Month|@Month')
                        .line('Year|@Year')) + \
         scale_color_discrete() + scale_fill_discrete() + \
         facet_grid(x='Month', y='Year') + \
         xlab('Monat') + ylab('Jahr') + \
         ggtitle('Anzahl der "positive" Tweets nach Monat und Jahr') + \
         ggsize(800, 400) + \
         theme_classic() + theme(legend_position='bottom')
    return p1


def plot_negative():
    df2 = get_filtered_sentiment_by_day(sentiment='negative')
    p2 = ggplot() + \
         geom_histogram(aes(y='number_of_tweets', x='Day', color='Year', fill='Year'),
                        stat='identity',
                        data=df2, bins=31, size=.5, alpha=.5,
                        tooltips=layer_tooltips()
                        .line('number_of_tweets|@number_of_tweets')
                        .line('Day|@Day')
                        .line('Month|@Month')
                        .line('Year|@Year')) + \
         scale_color_discrete() + scale_fill_discrete() + \
         facet_grid(x='Month', y='Year') + \
         xlab('Monat') + ylab('Jahr') + \
         ggtitle('Anzahl der "negativen" Tweets nach Monat und Jahr') + \
         ggsize(800, 400) + \
         theme_classic() + theme(legend_position='bottom')
    return p2


def plot_avg_per_negative():
    df2 = get_filtered_sentiment_by_day(sentiment='negative')
    p2 = ggplot() + \
         geom_histogram(aes(y='Avg_Per', x='Day', color='Year', fill='Year'),
                        stat='identity',
                        data=df2, bins=31, size=.5, alpha=.5,
                        tooltips=layer_tooltips()
                        .line('Avg_Per|@Avg_Per')
                        .line('Day|@Day')
                        .line('Month|@Month')
                        .line('Year|@Year')) + \
         scale_color_discrete() + scale_fill_discrete() + \
         facet_grid(x='Month', y='Year') + \
         xlab('Monat') + ylab('Jahr') + \
         ggtitle('plot_avg_per_negative') + \
         ggsize(800, 400) + \
         theme_classic() + theme(legend_position='bottom')
    return p2


width = 1800
height = 500
bunch = GGBunch()
bunch.add_plot(plot_positive(), 0, 0, width, height)
bunch.add_plot(plot_negative(), 0, 500, width, height)
bunch.add_plot(plot_deaths(), 0, 1000, width, height)
bunch.add_plot(plot_avg_per_negative(), 0, 1500, width, height)
bunch.show()
