import os
from itertools import product
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import dateutil

from sabesp import sabesp_data

START_DATE: str = '2012-01-01'
END_DATE: str = '2021-03-31'
DF_PATH: str = f"{os.getcwd()}\\data\\dataset.pkl"

if not os.path.exists(DF_PATH):
    sabesp_data(START_DATE, END_DATE).to_pickle(DF_PATH)
RAW_DF: pd.DataFrame = pd.read_pickle(DF_PATH)


def heatmap(field: str, issum: bool = True) -> None:
    plt.figure()
    months: dict = {
        '01': 'Jan', '02': 'Fev', '03': 'Mar',
        '04': 'Abr', '05': 'Mai', '06': 'Jun',
        '07': 'Jul', '08': 'Ago', '09': 'Set',
        '10': 'Out', '11': 'Nov', '12': 'Dez',
    }
    df = RAW_DF[["date", field]]
    df['date'] = df['date'].apply(dateutil.parser.parse)
    df['month'] = df['date'].dt.strftime('%Y-%m')
    if issum:
        df = df.groupby("month").sum()
    else:
        df = df.groupby("month").mean()

    df['date'] = df.index
    df['year'] = [str(x)[2:4] for x in df['date']]
    df['month'] = [str(x)[-2:] for x in df['date']]
    df = df.pivot(
        index="year",
        columns="month",
        values=field,
    )
    df = df.rename(columns=months, inplace=False)
    sns.heatmap(df, linewidths=0, cmap="rocket").get_figure()
    plt.savefig(f"output\\heatmap {field}.png")


def joint_grid(x: str, y: str) -> None:
    plt.figure()
    sns.set_theme(style="white")
    df = RAW_DF[['year', x, y]]
    # df = df.query('year != 2014')
    # df = df.query('year != 2015')
    # normalized_df = (df-df.mean())/df.std()
    normalized_df = df  #(df-df.min())/(df.max()-df.min())
    g = sns.JointGrid(data=normalized_df, x=x, y=y, space=0)
    g.plot_joint(sns.kdeplot,
                 fill=True,
                 clip=((normalized_df[x].min(), normalized_df[x].max()),
                       (normalized_df[y].min(), normalized_df[y].max())),
                 thresh=0,
                 levels=100,
                 cmap="rocket")
    g.plot_marginals(sns.histplot, color="#03051A", alpha=1, bins=50)
    plt.savefig(f"output\\joint {x}-{y}.png")


def hist(x: str) -> None:
    plt.figure()
    df = RAW_DF
    sns.displot(df, x=x, bins=25, stat="density")
    plt.savefig(f"output\\hist {x}.png")


if __name__ == '__main__':
    fields: list = [
        r'Volume Operacional (hm3)', r'Volume Operacional (%)',
        r'Precipitacao (mm)', r'Vazao Afluente (m3_s)',
        r'Vazao de Retirada (m3_s)'
    ]

    aggr_methods: list = [False, False, True, False, False]

    for field, method in zip(fields, aggr_methods):
        heatmap(field, method)
    print("Heatmaps generated")

    for field in product(fields, fields):
        if field[0][0:4] != field[1][0:4]:
            joint_grid(field[0], field[1])
    print("jointgrids generated")

    for field in fields:
        hist(field)
    print("histograms generated")
