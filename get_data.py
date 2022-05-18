import os
import requests
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
import pandas as pd

url_traffic = 'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/json \
        ?limit=-1&offset=0&refine=t_1h%3A{}&timezone=UTC'

url_roadwork = 'https://opendata.paris.fr/api/v2/catalog/datasets/chantiers-perturbants/exports/json?limit=-1&offset=0&lang=fr&timezone=UTC'


def save_to_hdfs(path):
    # TODO : not sure about this path
    hdfs_path = 'data/gp8/raw'

    put = Popen(["hdfs", "dfs", "-put", path, hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()

    os.system(f'rm {path}')


def get_traffic_data(day):
    path = f"{day}.parquet"
    response = requests.get(url_traffic.format(yesterday))
    df = pd.json_normalize(response.json())
    df.to_parquet(path, index=False)

    return path


def get_roadwork_data():
    path = 'data_travaux.parquet'
    response = requests.get(url_roadwork)
    df = pd.json_normalize(response.json())
    df.to_parquet(path, index=False)

    return path


if __name__ == '__main__':
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    traffic_path = get_traffic_data(yesterday)
    save_to_hdfs(traffic_path)

    roadwork_path = get_roadwork_data()
    save_to_hdfs(roadwork_path)
