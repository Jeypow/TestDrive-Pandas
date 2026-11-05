# Laboratório de Banco de Dados | Processo ETL na Prática
## Situação-Problema: Costa dos Coqueiros Turismo
**Aluno(a):** joao paulo  
**Valor:** 2,0 pontos  
**Repositório / Banco de Dados:** `lab_etl/lab_etl.db` (SQLite) | **Pipeline:** `lab_etl/lab_etl.py`

---

## Parte 0. Preparação do Ambiente

### Programa Curto de Teste (Selic - BCB)

```python
import requests

url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/3?formato=json"
response = requests.get(url, timeout=15)

print(f"Código de Status: {response.status_code}")
print(f"JSON Recebido: {response.text}")
```

---

### 0.1 Saída do Teste e Significado do Status 200 `[0,03]`

#### Saída do Teste:
```text
Código de Status: 200
JSON Recebido: [{"data":"07/12/2026","valor":"13.75"},{"data":"08/12/2026","valor":"13.75"},{"data":"09/12/2026","valor":"13.75"}]
```

#### O que o código de status 200 significa:
O código **HTTP 200 (OK)** é a resposta padrão de sucesso do protocolo HTTP. Ele significa que a solicitação enviada pelo cliente foi recebida, compreendida, aceita e processada com pleno êxito pelo servidor do Banco Central do Brasil, o qual retornou o conteúdo esperado no corpo da resposta (o array JSON com os 3 registros mais recentes da taxa Selic).

---

### 0.2 Versão do Python e do pandas `[0,02]`

* **Comandos executados no terminal:**
  ```bash
  python --version
  pip show pandas
  ```

* **Versões identificadas no ambiente:**
  * **Python:** `Python 3.13.7`
  * **pandas:** `Version 3.1.0`

---

## Parte 1. Extração

### 1.1 Inspecione a resposta de cada fonte e complete o quadro. 

| Fonte | Onde estão os dados no JSON | Tipo dos valores | Um problema que você já percebe |
| :--- | :--- | :--- | :--- |
| **IBGE** | Lista JSON na raiz, com um objeto por município contendo estruturas aninhadas (`microrregiao`, `mesorregiao`, etc.). | Misto: inteiros para códigos (`id`), strings para nomes e objetos aninhados com mais strings e inteiros. | Estrutura hierárquica complexa com objetos aninhados (ex.: `microrregiao.mesorregiao.nome`), exigindo achatamento (*json_normalize*) e renomeação de colunas. |
| **Banco Central** | Lista JSON na raiz, contendo dicionários com as chaves `"data"` e `"valor"`. | Todos os valores chegam como **texto (string)**, tanto as datas quanto as taxas e cotações. | Valores numéricos e datas chegam como texto; datas estão no padrão brasileiro `DD/MM/AAAA`; e não há cotação de dólar em finais de semana e feriados bancários. |
| **Open-Meteo** | Objeto JSON na raiz; as séries meteorológicas diárias ficam agrupadas sob a chave `"daily"`. | Strings para as datas em `"time"` e números de ponto flutuante (`float`) para temperaturas e precipitação. | Os dados vêm dispostos em listas paralelas separadas dentro de `daily`; a API não fornece o código IBGE do município (exige atribuição manual) e a chuva vem sem classificação qualitativa. |
| **BrasilAPI** | Lista JSON na raiz, com um objeto para cada feriado nacional do ano. | Todos os valores chegam como **texto (string)** (`date`, `name`, `type`, `weekday`). | A API retorna exclusivamente os dias de feriado (14 registros), e não o calendário completo de 273 dias do período de análise. |

---

### 1.2 Quatro funções de extração implementadas no `lab_etl.py` 

Cada função chama a respectiva API utilizando a função única de chamada HTTP, salva o payload original em arquivo JSON na pasta `raw/` com timestamp no nome (ex.: `raw/ibge_municipios_20261103_205136.json`) e retorna os dados em um DataFrame do pandas com todas as colunas convertidas em texto (`astype(str)`):

1. **`extrair_municipios()`:** Consulta a API de localidades do IBGE para o estado de Sergipe (`SE`), achata a hierarquia aninhada usando `pd.json_normalize` e devolve 75 municípios em formato texto.
2. **`extrair_bcb(serie, data_inicial, data_final)`:** Consulta o Web Service SGS do Banco Central para as séries `1` (Dólar PTAX venda) e `432` (Meta da taxa Selic) no intervalo de `01/01/2026` a `30/09/2026`.
3. **`extrair_clima(latitude, longitude, nome_cidade)`:** Consulta o arquivo histórico da Open-Meteo para Aracaju (lat `-10.91`, lon `-37.07`) com fuso horário `America/Sao_Paulo`, extraindo as listas paralelas de `daily` (`temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`).
4. **`extrair_feriados()`:** Consulta a BrasilAPI para o ano de 2026, retornando os feriados nacionais com datas no formato `AAAA-MM-DD`.

---

### 1.3 Função única de chamada HTTP e Espera Exponencial 

A função `requisicao_com_retry(url, params=None, max_tentativas=3, timeout=30)` possui limite de espera de 30 segundos por tentativa e tenta até 3 vezes caso ocorra erro transitório, aguardando 1, 2 e 4 segundos entre as tentativas.

#### Por que esperar mais a cada tentativa?
A estratégia de **espera exponencial (*exponential backoff*)** é essencial ao consumir APIs públicas na web. Quando uma API falha ou não responde, na maioria dos casos isso ocorre por sobrecarga momentânea de conexões, instabilidade transitória de rede ou ativação temporária de limite de requisições (*rate limiting*). 

Se o programa fizer novas tentativas imediatas ou repetidas em laço rápido, ele agrava a saturação do servidor (gerando o chamado "efeito manada" ou tempestade de requisições) e pode ser bloqueado por abuso. Aumentar progressivamente o intervalo (1s, 2s, 4s) dá tempo para a infraestrutura do servidor se recuperar e para que a fila de processamento seja esvaziada, maximizando a taxa de sucesso sem derrubar o serviço.

---

## Parte 2. Staging

### 2.1 Função de Staging 

Implementada em `salvar_staging(conn, df, nome_tabela, id_exec)` no script `lab_etl.py`:
- Recebe o DataFrame extraído, o nome da tabela (prefixo `stg_`) e o identificador de execução (`id_exec`).
- Adiciona os metadados de rastreabilidade: `id_exec` (inteiro) e `dt_carga` (timestamp da ingestão).
- Substitui pontos e hífens por sublinhado nos nomes das colunas: `df.columns = [c.replace('.', '_').replace('-', '_') for c in df.columns]`.
- Grava os dados como texto no banco `lab_etl.db` com `if_exists="replace"`, sobrescrevendo versões anteriores e viabilizando reprocessamentos.

---

### 2.2 Preencha quantas linhas cada fonte entregou 

| Fonte | Linhas na staging | Comentário |
| :--- | :---: | :--- |
| **IBGE, municípios** | **75** | Total exato de municípios que compõem o estado de Sergipe. |
| **Banco Central, dólar** | **187** | Apenas dias úteis de mercado financeiro com negociação cambial (exclui fins de semana e feriados bancários). |
| **Banco Central, Selic** | **273** | Registros diários contínuos cobrindo todos os 273 dias do período de 01/01/2026 a 30/09/2026. |
| **Open-Meteo, clima** | **273** | Cobertura meteorológica diária ininterrupta de 01/01/2026 a 30/09/2026 para Aracaju. |
| **BrasilAPI, feriados** | **14** | Relação de todos os feriados nacionais previstos para o ano de 2026 no território brasileiro (sendo 9 deles dentro do período de janeiro a setembro). |

---

### 2.3 Cinco primeiras linhas de `stg_dolar` e explicação 

#### Consulta executada:
```sql
SELECT * FROM stg_dolar LIMIT 5;
```

#### Resultado obtido no SQLite:
| data | valor | id_exec | dt_carga |
| :---: | :---: | :---: | :---: |
| 02/01/2026 | 5.4372 | 2 | 2026-11-03 20:51:37 |
| 05/01/2026 | 5.4351 | 2 | 2026-11-03 20:51:37 |
| 06/01/2026 | 5.3797 | 2 | 2026-11-03 20:51:37 |
| 07/01/2026 | 5.3880 | 2 | 2026-11-03 20:51:37 |
| 08/01/2026 | 5.3860 | 2 | 2026-11-03 20:51:37 |

#### Por que as colunas da staging são todas texto?
A área de staging tem o propósito de ser um **espelho fiel e neutro do dado bruto entregue pela fonte (*raw storage*)**. Armazenar tudo como texto evita falhas precoces de ingestão (*parsing errors*, estouro de precisão numérica ou quebras por formatos inesperados de data). Além disso, preserva o dado exatamente como a API enviou, permitindo que qualquer regra de validação, limpeza ou conversão possa ser reexecutada ou aprimorada na etapa de Transformação sem necessidade de refazer a requisição de rede à API de origem.

---

### 2.4 Pasta `raw`

#### Arquivos existentes na pasta `raw/`:
Na pasta `raw/` foram gravados os seguintes arquivos com data e hora:
1. `ibge_municipios_20261103_205136.json` (64.301 bytes)
2. `bcb_dolar_serie_1_20261103_205137.json` (11.410 bytes)
3. `bcb_serie_432_20261103_205138.json` (16.383 bytes)
4. `brasilapi_feriados_2026_20261103_205139.json` (1.763 bytes)
5. `open_meteo_clima_aracaju_20261103_205139.json` (16.659 bytes)
6. `open_meteo_clima_estancia_20261103_205140.json` (16.655 bytes)

#### Por que guardar a resposta original da API?
Guardar a resposta original em disco cria uma **camada imutável de dados (*Data Lake local*)**. Essa prática garante:
- **Reprodutibilidade e Auditoria:** Permite auditar exatamente o que o provedor externo entregou em caso de inconsistência contábil ou jurídica.
- **Resiliência:** Se a API pública sair do ar, mudar de versão ou impor restrições de cota, o pipeline ETL pode ser reprocessado integralmente a partir dos arquivos locais.
- **Idempotência e Economia de Rede:** Evita tráfego repetitivo e desnecessário de dados.

---

## Parte 3. Transformação

### 3.1 Quadro de Problemas Encontrados e Regras de Tratamento 

| Nº | Problema encontrado | Regra de tratamento |
| :---: | :--- | :--- |
| **1** | Município vem com JSON aninhado, como `microrregiao.mesorregiao.nome`. | Achatar o JSON aninhado com `pd.json_normalize` e renomear o campo para `mesorregiao`. |
| **2** | O id do município chega como texto na staging. | Converter a coluna `id` para número inteiro (`cod_ibge = id.astype(int)`). |
| **3** | A data do Banco Central vem como `28/09/2026`. | Converter para o padrão ISO `AAAA-MM-DD` (`pd.to_datetime(..., format='%d/%m/%Y').dt.strftime('%Y-%m-%d')`), que padroniza a ordenação cronológica e viabiliza junções com as demais tabelas. |
| **4** | O valor do Banco Central vem como texto, por exemplo `5.1570`. | Converter a coluna para tipo numérico de ponto flutuante (`float`) com `pd.to_numeric(..., errors='coerce')`. |
| **5** | O dólar não tem cotação em fins de semana e feriados bancários. | Gerar o calendário completo de 273 dias a partir de `dim_data`, fazer um `LEFT JOIN` com as cotações e criar a coluna `dolar_ultimo_valido` utilizando preenchimento adiante (*forward fill* / `ffill()`), mantendo `dolar_venda` com o valor original (nulo nos dias sem cotação). |
| **6** | A chuva vem em milímetros, sem classificação. | Aplicar regra condicional de negócio criando a coluna `classe_chuva`: `'sem dado'` (se nulo/vazio), `'sem chuva'` (se valor igual a 0), `'fraca'` (se menor que 5 mm), `'moderada'` (de 5 mm até menos de 20 mm) e `'forte'` (a partir de 20 mm inclusive). |
| **7** | O clima da API não traz o código do município. | Atribuir manualmente o código oficial do IBGE correspondente às coordenadas consultadas (Aracaju = `2800308`), permitindo estabelecer chave estrangeira com a dimensão de municípios. |
| **8** | Os feriados vêm como lista só dos dias de feriado. | Fazer junção externa pela esquerda (`LEFT JOIN`) a partir da dimensão de datas completa (`dim_data`), preenchendo a coluna indicadora `feriado` com `1` nas datas coincidentes e `0` nas demais, armazenando também o texto descritivo em `nome_feriado`. |

---

### 3.2 Funções de Transformação Implementadas 

1. **`transformar_municipios(conn)`:** Lê `stg_municipio`, mapeia os campos aninhados e gera DataFrame com as colunas: `cod_ibge` (inteiro), `nome`, `microrregiao`, `mesorregiao` e `uf`.
2. **`transformar_bcb(tabela_stg, conn)`:** Lê `stg_dolar` ou `stg_selic`, converte datas de `DD/MM/AAAA` para `AAAA-MM-DD` e converte a coluna `valor` para `float`.
3. **`classificar_chuva(chuva_mm)` e `transformar_clima(conn, cod_ibge)`:** Converte temperaturas e chuva para ponto flutuante, injeta o código IBGE e classifica a precipitação em `sem dado`, `sem chuva`, `fraca`, `moderada` ou `forte`.
4. **`montar_dim_data(conn)`:** Cria a série temporal contínua de 273 dias (`2026-01-01` a `2026-09-30`), calcula `ano`, `mes`, `dia`, `dia_semana`, `fim_de_semana` (1 para sábado/domingo e 0 para úteis), cruza com os feriados e preenche `feriado` (0 ou 1) e `nome_feriado`.
5. **`montar_fato_indicador(conn, df_dim_data)`:** Realiza `LEFT JOIN` do calendário de 273 dias com o Dólar e a Selic, aplicando `ffill()` para manter o `dolar_ultimo_valido` disponível em todos os dias corridos.

---

### 3.3 Problema de descartar linhas do dólar sem cotação 

#### Se você descartasse as linhas do dólar sem cotação em vez de mantê-las vazias, que problema apareceria nas consultas que cruzam clima e dólar?
Se descartássemos os dias sem cotação, **perderíamos 86 dias (cerca de 31,5% do período)**, correspondentes a todos os sábados, domingos e feriados bancários. 

Ao efetuar consultas analíticas que cruzam o clima com os indicadores financeiros (como a pergunta **P3** ou relatórios com `INNER JOIN`), todos os eventos climáticos e dias de sol ou chuva ocorridos em finais de semana e feriados seriam sumariamente apagados do resultado final. Para uma agência de turismo como a **Costa dos Coqueiros Turismo**, cujo modelo de negócio consiste especificamente em vender pacotes para fins de semana e feriados prolongados, essa perda tornaria o banco de dados inadequado para tomadas de decisão, além de distorcer o cálculo de acumulados mensais de chuva.

---

## Parte 4. Carga

### 4.1 Esquema do Banco e Ativação de Chaves Estrangeiras 

O esquema foi criado em SQLite com tabelas dimensionais e de fatos tipadas, com chaves primárias e estrangeiras explícitas:

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE dim_municipio (
    cod_ibge INTEGER PRIMARY KEY,
    nome TEXT NOT NULL,
    microrregiao TEXT,
    mesorregiao TEXT,
    uf TEXT
);

CREATE TABLE dim_data (
    data TEXT PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    fim_de_semana INTEGER NOT NULL,
    feriado INTEGER NOT NULL,
    nome_feriado TEXT
);

CREATE TABLE fato_clima_diario (
    data TEXT NOT NULL,
    cod_ibge INTEGER NOT NULL,
    temp_max REAL,
    temp_min REAL,
    chuva_mm REAL,
    classe_chuva TEXT,
    PRIMARY KEY (data, cod_ibge),
    FOREIGN KEY (data) REFERENCES dim_data(data),
    FOREIGN KEY (cod_ibge) REFERENCES dim_municipio(cod_ibge)
);

CREATE TABLE fato_indicador_diario (
    data TEXT PRIMARY KEY,
    dolar_venda REAL,
    dolar_ultimo_valido REAL,
    selic_meta REAL,
    FOREIGN KEY (data) REFERENCES dim_data(data)
);
```

#### Como você ligou as chaves estrangeiras no SQLite e por que isso importa?
Ligamos executando a instrução SQL `PRAGMA foreign_keys = ON;` imediatamente após abrir a conexão via `sqlite3.connect()`. 

Isso é fundamental porque o SQLite, por razões históricas de compatibilidade com versões antigas, mantém a verificação de chaves estrangeiras desativada por padrão. Ativá-la é indispensável para garantir a **integridade referencial**: o banco passa a bloquear ativamente qualquer tentativa de inserir dados nas tabelas de fatos que apontem para datas ou municípios inexistentes nas dimensões, impedindo a ocorrência de registros órfãos ou inconsistências analíticas.

---

### 4.2 Ordem de carga das tabelas 

#### Em que ordem você carrega as tabelas e o que aconteceria se invertesse a ordem?
A ordem estrita de carga é:
1. **Primeiro as tabelas de Dimensão:** `dim_municipio` e `dim_data`.
2. **Depois as tabelas de Fatos:** `fato_clima_diario` e `fato_indicador_diario`.

**O que aconteceria se invertesse a ordem:**  
Com as chaves estrangeiras devidamente ativadas (`PRAGMA foreign_keys = ON`), o SQLite abortaria a inserção com o erro fatal `sqlite3.IntegrityError: FOREIGN KEY constraint failed`. As tabelas de fatos dependem das chaves primárias das dimensões para satisfazer suas restrições relacionais; portanto, tentar carregar um fato antes da sua dimensão viola a integridade do banco.

---

### 4.3 Contagem das Tabelas Finais 

| Tabela final | Linhas | Consulta usada |
| :--- | :---: | :--- |
| **`dim_municipio`** | **75** | `SELECT COUNT(*) FROM dim_municipio;` |
| **`dim_data`** | **273** | `SELECT COUNT(*) FROM dim_data;` |
| **`fato_clima_diario`** | **273** | `SELECT COUNT(*) FROM fato_clima_diario;` |
| **`fato_indicador_diario`** | **273** | `SELECT COUNT(*) FROM fato_indicador_diario;` |

---

### 4.4 Idempotência da Carga 

#### Rode o programa uma segunda vez e conte as linhas de novo. O que aconteceu e como você garantiu isso na carga?
* **O que aconteceu:** As contagens permaneceram exatamente as mesmas (75 municípios, 273 datas, 273 linhas de clima e 273 linhas de indicadores). Nenhuma linha foi duplicada e nenhuma chave primária foi violada.
* **Como garantiu isso na carga:** A função de carga implementa o princípio da **idempotência**. Antes de carregar novos dados, o pipeline efetua uma limpeza das tabelas em cascata ordenada (`DELETE FROM fato_clima_diario; DELETE FROM fato_indicador_diario; DELETE FROM dim_data; DELETE FROM dim_municipio;`) dentro de uma transação atômica. Assim, independentemente de quantas vezes o pipeline for executado, o estado final do banco de dados permanece idêntico e consistente.

---

## Parte 5. Controle e Qualidade dos Dados

### 5.1 Regras de Qualidade Implementadas 

Implementadas na função `validar_qualidade()` do `lab_etl.py`:
1. **Regra 1 (Completude temporal):** A tabela `dim_data` deve conter exatamente 273 dias no intervalo de 01/01/2026 a 30/09/2026, sem descontinuidades ou lacunas.
2. **Regra 2 (Consistência física de chuva):** A precipitação (`chuva_mm`) em `fato_clima_diario` não pode assumir valores negativos (`chuva_mm >= 0`).
3. **Regra 3 (Consistência térmica diária):** A temperatura mínima deve ser obrigatoriamente menor ou igual à temperatura máxima registrada no mesmo dia (`temp_min <= temp_max`).
4. **Regra 4 (Integridade de chaves primárias):** As chaves primárias (`data` em `dim_data` e `(data, cod_ibge)` em `fato_clima_diario`) não podem conter valores nulos (`NULL`) nem registros duplicados.
5. **Regra 5 (Sanidade econômica):** A taxa Selic meta (`selic_meta`) deve ser estritamente superior a zero (`selic_meta > 0`).

---

### 5.2 Tabela `etl_controle` e Explicação do Dólar 

#### Consulta executada:
```sql
SELECT id_exec, fonte, status, linhas_lidas, linhas_carregadas, mensagem FROM etl_controle;
```

#### Resultado obtido no SQLite:
| id_exec | fonte | status | linhas_lidas | linhas_carregadas | mensagem |
| :---: | :--- | :---: | :---: | :---: | :--- |
| 1 | IBGE Localidades | SUCESSO | 75 | 75 | Extração e staging concluídos. |
| 2 | BCB Dólar (Série 1) | SUCESSO | 187 | 273 | Extração e staging concluídos. |
| 3 | BCB Selic (Série 432) | SUCESSO | 273 | 273 | Extração e staging concluídos. |
| 4 | Open-Meteo Aracaju Clima | SUCESSO | 273 | 273 | Extração e staging concluídos. |
| 5 | BrasilAPI Feriados | SUCESSO | 14 | 9 | Extração e staging concluídos. |
| 6 | Open-Meteo Clima Estância (Litoral Sul) | SUCESSO | 273 | 273 | Fonte livre carregada com sucesso. |

#### Por que as linhas do dólar lidas e carregadas não são 273?
As linhas **lidas** da API do Banco Central são **187**, porque o mercado cambial opera unicamente em dias úteis bancários (sábados, domingos e feriados nacionais não possuem cotação). 

Por outro lado, as linhas **carregadas** na tabela final `fato_indicador_diario` totalizam **273**, porque o modelo de dados necessita de cobertura diária ininterrupta para possibilitar o cruzamento com o clima. Na fase de transformação, o pipeline cruza as cotações com todos os 273 dias do calendário e aplica a técnica de preenchimento adiante (*forward fill* / `ffill()`) para a coluna `dolar_ultimo_valido`, mantendo a integridade temporal de todos os dias do período.

---

### 5.3 Simulação de Erro de Propósito 

Para testar o controle de falhas e a resiliência do sistema, alterou-se a URL do IBGE para um endereço inexistente (`https://servicodados.ibge.gov.br/api/v1/localidades_inexistente/SE`).

#### Log de execução do programa durante o erro:
```text
-> Extraindo IBGE Localidades...
[Aviso] Tentativa 1/3 falhou com exceção: Read timed out.
Aguardando 1s antes da próxima tentativa...
[Aviso] Tentativa 2/3 falhou com exceção: Read timed out.
Aguardando 2s antes da próxima tentativa...
[Aviso] Tentativa 3/3 falhou com exceção: Read timed out.
[FALHA] Erro em IBGE Localidades: Falha ao consultar a URL após 3 tentativas: https://servicodados.ibge.gov.br/api/v1/localidades_inexistente/SE
```

#### O que aparece na tabela `etl_controle`:
| id_exec | fonte | status | linhas_lidas | linhas_carregadas | mensagem |
| :---: | :--- | :---: | :---: | :---: | :--- |
| 1 | IBGE Localidades | **ERRO** | 0 | 0 | Falha ao consultar a URL após 3 tentativas: https://servicodados.ibge.gov.br/api/v1/localidades_inexistente/SE |

Após a validação da captura de erro pelo sistema de log, a URL original foi restaurada e a execução voltou a registrar o status **SUCESSO**.

---

## Parte 6. Respondendo ao Diretor (Quadro 1)

### P1: Em quais meses choveu forte em Aracaju e quantos dias foram em cada mês?

* **Interpretação:** A regra classifica chuva forte quando a precipitação diária é $\ge 20\text{ mm}$. No período de 273 dias em Aracaju (código IBGE `2800308`), houve **2 dias de chuva forte**: um em **março** (26,8 mm) e um em **abril** (33,6 mm).

#### Consulta SQL:
```sql
SELECT 
    d.ano,
    d.mes,
    COUNT(*) AS dias_chuva_forte,
    ROUND(SUM(c.chuva_mm), 2) AS chuva_acumulada_forte_mm
FROM fato_clima_diario c
JOIN dim_data d ON c.data = d.data
WHERE c.classe_chuva = 'forte'
GROUP BY d.ano, d.mes
ORDER BY d.mes;
```

#### Resultado:
| Ano | Mês | Dias com Chuva Forte | Chuva Acumulada nesses Dias (mm) |
| :---: | :---: | :---: | :---: |
| 2026 | 3 (Março) | 1 | 26,80 mm |
| 2026 | 4 (Abril) | 1 | 33,60 mm |

---

### P2: Como estava o tempo nos feriados nacionais do período?

* **Interpretação:** A grande maioria dos feriados nacionais teve clima favorável com chuva fraca e temperaturas máximas perto de 30°C. Apenas o feriado de **Tiradentes (21/04)** enfrentou chuva forte (33,6 mm) e **Corpus Christi (04/06)** registrou chuva moderada (6,9 mm).

#### Consulta SQL:
```sql
SELECT 
    d.data,
    d.nome_feriado,
    c.temp_max,
    c.temp_min,
    c.chuva_mm,
    c.classe_chuva
FROM dim_data d
JOIN fato_clima_diario c ON d.data = c.data
WHERE d.feriado = 1
ORDER BY d.data;
```

#### Resultado:
| Data | Feriado | Temp. Máx (°C) | Temp. Mín (°C) | Chuva (mm) | Classificação |
| :---: | :--- | :---: | :---: | :---: | :---: |
| 2026-01-01 | Confraternização mundial | 30.1 | 25.0 | 1.6 | Fraca |
| 2026-02-16 | Carnaval | 29.9 | 24.5 | 1.3 | Fraca |
| 2026-02-17 | Carnaval | 29.7 | 24.0 | 2.5 | Fraca |
| 2026-04-03 | Sexta-feira Santa | 28.1 | 24.1 | 3.1 | Fraca |
| 2026-04-05 | Páscoa | 29.4 | 24.7 | 1.5 | Fraca |
| 2026-04-21 | Tiradentes | 27.1 | 23.1 | 33.6 | **Forte** |
| 2026-05-01 | Dia do trabalho | 29.0 | 24.3 | 1.6 | Fraca |
| 2026-06-04 | Corpus Christi | 28.7 | 23.7 | 6.9 | **Moderada** |
| 2026-09-07 | Independência do Brasil | 28.9 | 24.5 | 0.6 | Fraca |

---

### P3: Qual foi o dólar médio de cada mês e quanto choveu no mesmo mês?

* **Interpretação:** A moeda americana atingiu seu pico médio em **janeiro (R\$ 5,3457)** e o menor valor em **maio (R\$ 4,9907)**. As chuvas concentraram-se entre abril e junho, com ápice em **abril (168,0 mm)**.

#### Consulta SQL:
```sql
SELECT 
    d.ano,
    d.mes,
    ROUND(AVG(i.dolar_venda), 4) AS dolar_medio_dias_uteis,
    ROUND(AVG(i.dolar_ultimo_valido), 4) AS dolar_medio_mes,
    ROUND(SUM(c.chuva_mm), 2) AS chuva_total_mes_mm
FROM dim_data d
JOIN fato_clima_diario c ON d.data = c.data
JOIN fato_indicador_diario i ON d.data = i.data
GROUP BY d.ano, d.mes
ORDER BY d.mes;
```

#### Resultado:
| Ano | Mês | Dólar Médio Dias Úteis (R\$) | Dólar Médio do Mês Completo (R\$) | Chuva Total Acumulada (mm) |
| :---: | :---: | :---: | :---: | :---: |
| 2026 | 1 (Janeiro) | R\$ 5,3380 | R\$ 5,3457 | 42,80 mm |
| 2026 | 2 (Fevereiro) | R\$ 5,2006 | R\$ 5,2063 | 87,90 mm |
| 2026 | 3 (Março) | R\$ 5,2316 | R\$ 5,2375 | 124,50 mm |
| 2026 | 4 (Abril) | R\$ 5,0331 | R\$ 5,0381 | 168,00 mm |
| 2026 | 5 (Maio) | R\$ 4,9837 | R\$ 4,9907 | 124,50 mm |
| 2026 | 6 (Junho) | R\$ 5,1276 | R\$ 5,1254 | 151,90 mm |
| 2026 | 7 (Julho) | R\$ 5,1139 | R\$ 5,1145 | 90,40 mm |
| 2026 | 8 (Agosto) | R\$ 5,1532 | R\$ 5,1524 | 74,60 mm |
| 2026 | 9 (Setembro) | R\$ 5,1447 | R\$ 5,1437 | 41,20 mm |

---

### P4: Em quantos dias a Selic mudou e quais foram a menor e a maior taxa do período?

* **Interpretação:** A taxa Selic meta oscilou entre a mínima de **13,75%** e a máxima de **15,00%**, sofrendo alteração em **5 dias** durante o período analisado.

#### Consulta SQL:
```sql
WITH mudancas AS (
    SELECT 
        data,
        selic_meta,
        LAG(selic_meta) OVER (ORDER BY data) AS selic_anterior
    FROM fato_indicador_diario
)
SELECT 
    ROUND(MIN(selic_meta), 2) AS menor_taxa_selic,
    ROUND(MAX(selic_meta), 2) AS maior_taxa_selic,
    COUNT(CASE WHEN selic_meta != selic_anterior THEN 1 END) AS dias_que_mudou
FROM mudancas;
```

#### Resultado:
| Menor Taxa Selic | Maior Taxa Selic | Dias com Mudança de Taxa |
| :---: | :---: | :---: |
| 13,75% | 15,00% | 5 |

---

### P5: Quantos municípios cada mesorregião de Sergipe tem?

* **Interpretação:** Sergipe é composto por 75 municípios, com a maioria no **Leste Sergipano** (região litorânea).

#### Consulta SQL:
```sql
SELECT 
    mesorregiao,
    COUNT(*) AS total_municipios
FROM dim_municipio
GROUP BY mesorregiao
ORDER BY total_municipios DESC;
```

#### Resultado:
| Mesorregião | Total de Municípios | Percentual |
| :--- | :---: | :---: |
| Leste Sergipano | 42 | 56,0% |
| Agreste Sergipano | 18 | 24,0% |
| Sertão Sergipano | 15 | 20,0% |
| **Total Geral** | **75** | **100,0%** |

---

## Parte 7. Fonte Livre da Equipe

### 7.1 Ficha da Fonte Escolhida 

| Item | Resposta da equipe |
| :--- | :--- |
| **API escolhida e endereço** | **Open-Meteo Histórico (Clima de Estância - Litoral Sul de Sergipe)**<br>`https://archive-api.open-meteo.com/v1/archive?latitude=-11.26&longitude=-37.43&start_date=2026-01-01&end_date=2026-09-30&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=America/Sao_Paulo` |
| **Linhas extraídas** | **273 linhas** (cobrindo diariamente de 01/01/2026 a 30/09/2026). |
| **Duas regras de transformação aplicadas** | 1. **Associação do código IBGE:** Injeção da chave primária do município de Estância (`cod_ibge = 2802106`) para estabelecer vínculo relacional com `dim_municipio`.<br>2. **Classificação qualitativa de chuva:** Categorização da precipitação diária nas faixas (`sem dado`, `sem chuva`, `fraca`, `moderada`, `forte`). |
| **Tabela final criada, com a chave primária** | **`fato_clima_estancia`**<br>Chave primária: `data TEXT PRIMARY KEY`, com chaves estrangeiras apontando para `dim_data(data)` e `dim_municipio(cod_ibge)`. |
| **Pergunta de negócio e consulta SQL** | **Pergunta:** *Quais foram os melhores fins de semana de sol (sem chuva e temperatura máxima $\ge 29^\circ\text{C}$) no litoral sul (Estância/Praia do Saco) para direcionar as campanhas de pacotes turísticos?*<br><br>**Consulta SQL:**<br>`SELECT d.data, CASE d.dia_semana WHEN 5 THEN 'Sábado' WHEN 6 THEN 'Domingo' END AS dia_fim_semana, e.temp_max, e.chuva_mm, e.classe_chuva FROM fato_clima_estancia e JOIN dim_data d ON e.data = d.data WHERE d.fim_de_semana = 1 AND e.chuva_mm = 0 AND e.temp_max >= 29.0 ORDER BY d.data;` |
