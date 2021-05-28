import json
from datetime import datetime

import pandas as pd
import urllib3
from fastcore.all import typedispatch

URL = r'http://mananciais.sabesp.com.br/api/Mananciais/RepresasSistemasNivel'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def sabesp_data(start: str, end: str) -> pd.DataFrame:
    '''
    Get the system data from SABESP as Pandas DataFrame format

    Parameters:
    start(string): The start date for the query
    end(string): The end date for the query

    Start and end dates should be in ISO 8601 format (yyyy-mm-dd)
    '''
    
    '''If the date range is bigger than a year, the "datesplit" function
    is called before calling "call_sabesp"'''
    if int(end[:4]) - int(start[:4]) <= 1:
        dates = {'start': start, 'end': end}
        return (get_data(dates))
    return datesplit(start, end)


def datesplit(start: str, end: str):
    '''split the range informed by the user in a list of small ranges
    as the Sabesp servers cannot deal with date ranges bigger than 2 years'''
    original_start = datetime.strptime(start, "%Y-%m-%d")
    original_end = datetime.strptime(end, "%Y-%m-%d")
    dates: list = []
    dates.append({'start': start, 'end': f'{original_start.year}-12-31'})
    y = original_start.year + 1
    while y < original_end.year:
        dates.append({'start': f'{y}-01-01', 'end': f'{y}-12-31'})
        y += 1
    dates.append({'start': f'{original_end.year}-01-01', 'end': end})
    return get_data(dates)


@typedispatch
def get_data(dates: dict):
    return build_df(call_sabesp(dates))


@typedispatch
def get_data(dates: list):
    return build_df([day for subset
                    in list(map(call_sabesp, dates))
                    for day in subset])


def call_sabesp(dates: dict):
    start: str = dates['start']
    end: str = dates['end']
    values: list = []
    http = urllib3.PoolManager(cert_reqs='CERT_NONE').request
    try:
        print(f'Getting data from {start} to {end}...')
        data: dict = json.loads(http('GET', f'{URL}/{start}/{end}/0').data)
    except Exception as e:
        print(e)
    data: dict = data.get('ReturnObj')
    for rec_gen, rec_esi in zip(data.get('ListaDadosSistema'),
                                data.get('ListaDadosLocais')):
        date = datetime.strptime(rec_gen['Data'], "%Y-%m-%dT%H:%M:%S")
        year = date.year
        month = date.month
        day = date.day
        full_date = f'{year}/{month}/{day}'
        stored = rec_gen['objSistema']['VolumeOperacionalHm3']
        rain = rec_gen['objSistema']['Precipitacao']
        percent = rec_gen['objSistema']['VolumePorcentagem']
        input = rec_gen['objSistema']['VazaoNatural']
        output = rec_esi['Dados'][4]['Valor']
        values.append([
            year, month, day, full_date, stored, percent, rain, input, output
        ])
    return values


def build_df(records: list):
    print('Building DataFrame...')
    cols: list = [
        r'year', r'month', r'day', r'date',
        r'Volume Operacional (hm3)', r'Volume Operacional (%)',
        r'Precipitacao (mm)', r'Vazao Afluente (m3_s)',
        r'Vazao de Retirada (m3_s)'
    ]
    return pd.DataFrame(records, columns=cols)


if __name__ == '__main__':
    sdata = sabesp_data('2012-01-01', '2021-03-31')
    print(sdata)
    sdata.to_clipboard()
