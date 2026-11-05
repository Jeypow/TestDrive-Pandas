import pandas as pd



tabela1 = pd.read_csv('raw/chuva.csv')
tabela2 = pd.read_json('raw/dola.json')
tabela3 = pd.read_json('raw/feriados.json')
tabela4 = pd.read_json('raw/ibge_se.json')

tabela1 = tabela1.astype(str)
tabela2 = tabela2.astype(str)
tabela3 = tabela3.astype(str)
tabela4 = tabela4.astype(str)

