import pandas as pd


class ConfirmedCases:
    def __init__(self):
        self.url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'

    def fetch_time_series_covid19_confirmed_global(self, country):
        df1 = pd.read_csv(self.url)
        df1 = df1.rename(columns={'Country/Region': 'Country'})
        if country:
            df1 = df1[df1.Country == country]
        df1 = df1.iloc[:, 4:]
        df1 = df1.melt(id_vars=[], var_name="date", value_name="confirmed_since_2020_01_22")
        df1.date = pd.to_datetime(df1.date)
        df1['year'] = df1.date.dt.year
        df1['month'] = df1.date.dt.month
        df1['day'] = df1.date.dt.day
        df1.date = df1.date.dt.strftime('%Y-%m-%d')
        return df1

    def get_time_series_covid19_confirmed_for_us(self, year='', month=''):
        df2 = self.fetch_time_series_covid19_confirmed_global(country='US')
        values = df2.confirmed_since_2020_01_22.values
        zipped = list(zip(values[1:], values[:-1]))
        mapping = list(map(lambda p: p[0] - p[1], zipped))
        mapping.insert(0, values[0])
        df2 = df2.drop(columns=['confirmed_since_2020_01_22'])
        df2['daily_confirmed_cases'] = mapping
        if year:
            df2 = df2[df2.year == int(year)]
        if month:
            df2 = df2[df2.month == int(month)]
        return df2


if __name__ == '__main__':
    result = ConfirmedCases().get_time_series_covid19_confirmed_for_us('2021', '01')
    print(result)
