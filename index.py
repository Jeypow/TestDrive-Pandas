import pandas as pd

tabela1 = pd.read_csv('dados/chuva.csv')
tabela2 = pd.read_json('dados/dola')
tabela3 = pd.read_json('dados/feriados')
tabela4 = pd.read_json('dados/ibge_se')

print(tabela1)
