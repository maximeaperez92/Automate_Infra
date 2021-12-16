import requests
import pandas as pd

# https://opendata.paris.fr/api/v2/console

# Données traffic
# response = requests.get("https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/json?limit=-1&offset=0&refine=t_1h%3A2021%2F11%2F25&refine=libelle_nd_amont%3A14_Gouvion_St_Cyr&lang=fr&timezone=UTC")
response = requests.get("https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/json?limit=-1&offset=0&refine=t_1h%3A2021%2F11%2F25&lang=fr&timezone=UTC")
pd.json_normalize(response.json()).to_csv("data_traffic.csv", index=False)
print('Données traffic enregistrées')

# Données travaux
response = requests.get("https://opendata.paris.fr/api/v2/catalog/datasets/chantiers-perturbants/exports/json?limit=-1&offset=0&lang=fr&timezone=UTC")
pd.json_normalize(response.json()).to_csv("data_travaux.csv", index=False)
print('Données travaux enregistrées')


