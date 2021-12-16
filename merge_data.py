import pandas as pd


df_traffic = pd.read_csv('data_traffic.csv')
df_travaux = pd.read_csv('data_travaux.csv')[['voie', 'niveau_perturbation', 'date_debut',
                                              'date_fin', 'objet', 'description', 'impact_circulation_detail']]

# print(df_traffic.head())
# print(df_travaux.head())

df_traffic['libelle'] = df_traffic['libelle'].str.lower()

print(df_travaux['voie'].head())
df_travaux['voie'] = df_travaux['voie'].str.lower().str.split().str.join('_')
print(df_travaux['voie'].head())

# df_result = df_traffic.merge(df_travaux, left_on='libelle', right_on='voie', how='left')
# print(df_result)
