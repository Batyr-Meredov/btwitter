import os
import pandas as pd
from lets_plot import *
from lets_plot.mapping import as_discrete

from src.summary import utils

LetsPlot.setup_html()


class DeathCases:
    def __init__(self):
        self.format = '%b %d %Y'
        self.data_base_dir = f'{os.path.dirname(os.path.dirname(__file__))}/../batyr/data'
        self.owid_file = f'{self.data_base_dir}/owid-covid-data.csv'
        self.cdc_filename = f'{self.data_base_dir}/data_table_for_daily_death_trends.csv'
        self.url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

    def __fetch_time_series_covid19_death_global(self, country):
        df1 = pd.read_csv(self.url)
        df1 = df1.rename(columns={'Country/Region': 'Country'})
        if country:
            df1 = df1[df1.Country == country]
        df1 = df1.iloc[:, 4:]
        df1 = df1.melt(id_vars=[], var_name="date", value_name="death_since_2020_01_22")
        df1.date = pd.to_datetime(df1.date)
        df1['year'] = df1.date.dt.year
        df1['month'] = df1.date.dt.month
        df1['day'] = df1.date.dt.day
        return df1

    def get_time_series_covid19_death_for_country(self, year='', month='', country='US'):
        df2 = self.__fetch_time_series_covid19_death_global(country)
        values = df2.death_since_2020_01_22.values
        zipped = list(zip(values[1:], values[:-1]))
        mapping = list(map(lambda p: p[0] - p[1], zipped))
        mapping.insert(0, values[0])
        df2 = df2.drop(columns=['death_since_2020_01_22'])
        df2['ddc'] = mapping  # ddc = daily_death_cases
        if year:
            df2 = df2[df2.year == int(year)]
        if month:
            df2 = df2[df2.month == int(month)]
        return df2

    def get_time_series_covid19_death_for_us(self):
        return self.get_time_series_covid19_death_for_country(country='US')

    def get_time_series_covid19_death_for_spain(self):
        df = pd.read_csv(
            self.owid_file,
            names=['iso_code', 'date', 'count'],
            usecols=[0, 3, 8],
        )
        df = df[df.iso_code == 'ESP']
        df.date = pd.to_datetime(df.date)
        df["day"] = df.date.dt.day
        df["month"] = df.date.dt.month
        df["year"] = df.date.dt.year

        return df

    # https://covid.cdc.gov/covid-data-tracker/#trends_dailydeaths
    def read_death_cdc(self):
        df = pd.read_csv(
            self.cdc_filename,
            encoding="ISO-8859-1",
            header=0,
            na_filter=False,
            names=['date', 'count'],
            dtype={'date': str, 'count': int},
            usecols=[1, 2],
            parse_dates=['date'],
            infer_datetime_format=True,
            date_parser=lambda text: utils.try_parsing_date(text, self.format),
        )
        df["day"] = df.date.dt.day
        df["month"] = df.date.dt.month
        df["year"] = df.date.dt.year
        return df

    def plot_deaths_cdc(self, source='SPAIN'):
        df = self.get_time_series_covid19_death_for_spain() if (
                source == 'SPAIN') else self.get_time_series_covid19_death_for_us()

        print(df)
        __plot = ggplot(df, aes("date", "ddc")) + \
                 geom_line(aes(group="year", color=as_discrete("year")), size=0.5) + \
                 scale_x_datetime(breaks=df[df.date.dt.day == 1].date, format="%b %Y") + \
                 facet_grid(x="year", scales='free') + \
                 ggtitle("Datentabelle für tägliche Sterbetrends - Die Vereinigten Staaten") + \
                 ggsize(2000, 900) + \
                 theme(legend_position='bottom')

        filename = 'spain.html' if source == 'SPAIN' else 'usa.html'
        ggsave(__plot, filename, path='../../docs/images', iframe=False)
        __plot.show()

    def test(self):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        df = self.get_time_series_covid19_death_for_country()
        df = df[df['date'] >= '2021-08-01']
        df = df[df['date'] <= '2021-09-30']
        print(df)


if __name__ == '__main__':
    # result = DeathCases().get_time_series_covid19_death_for_us('2021', '01')
    # print(DeathCases().get_time_series_covid19_death_for_us())
    #DeathCases().plot_deaths_cdc('US')
    DeathCases().test()

    # DeathCases().plot_deaths_cdc()
