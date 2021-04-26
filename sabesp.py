import pandas as pd
import urllib3
import json
from fastcore.all import typedispatch

URL = r'http://mananciais.sabesp.com.br/api/Mananciais/RepresasSistemasNivel'


def pull_sabesp_data(start: str, end: str):
    '''
    Get the system data from SABESP as Pandas DataFrame format

    Parameters:
    start(string): The start date for the query
    end(string): The end date for the query

    Start and end dates should be in ISO 8601 format (yyyy-mm-dd)
    '''
    return(datesplit([start, end]))


def datesplit(datelist: list):
    return(request_data(dates))


@typedispatch
def build_df(dates: dict):
    pass


@typedispatch
def build_df(dates: list):
    for date in dates:
        pass


def get_data(dates: dict):
    start = dates['start']
    end = dates['end']
    http = urllib3.PoolManager().request
    data = json.loads(http('GET', f'{URL}/{start}/{end}/0').data)
    for day in data.get('ReturnObj').get('ListaDadosSistema'):
        pass

if __name__ == '__main__':
    dates = {'start': '2020-01-01', 'end': '2020-01-05'}
    print(get_data(dates))
