"""
Laboratório de Banco de Dados | Processo ETL na Prática
Situação-Problema: Costa dos Coqueiros Turismo
Arquivo: lab_etl.py
"""

import os
import sys
import time
import json
import sqlite3
from datetime import datetime
import pandas as pd
import requests

# Definição de diretórios base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Garante compatibilidade caso executado da raiz do repositório ou de dentro de lab_etl
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..")) if os.path.basename(BASE_DIR) == "lab_etl" else BASE_DIR
RAW_DIR = os.path.join(PROJECT_ROOT, "raw")
DB_PATH = os.path.join(BASE_DIR, "lab_etl.db")

# Garante criação da pasta raw
os.makedirs(RAW_DIR, exist_ok=True)


# ==============================================================================
# PARTE 0: Preparação do Ambiente e Teste Curto
# ==============================================================================
def teste_inicial_bcb():
    """
    Parte 0: Teste curto pedindo ao Banco Central os três últimos valores da meta Selic.
    Usa a série 432 com o complemento /ultimos/3 e formato=json.
    """
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/3?formato=json"
    print("\n--- [Parte 0] Teste de Conexão com a API do Banco Central (Selic) ---")
    try:
        resp = requests.get(url, timeout=15)
        print(f"URL: {url}")
        print(f"Código de Status HTTP: {resp.status_code}")
        print(f"JSON Recebido: {resp.text}")
        return resp.status_code, resp.json()
    except Exception as e:
        print(f"Erro no teste inicial: {e}")
        return None, None


# ==============================================================================
# PARTE 1: Extração
# ==============================================================================
def requisicao_com_retry(url, params=None, max_tentativas=3, timeout=30):
    """
    Parte 1.3: Função única de chamada HTTP com limite de espera de 30 segundos,
    verificação de status e até 3 tentativas com espera exponencial (1, 2 e 4 segundos).
    """
    delays = [1, 2, 4]
    for tentativa in range(1, max_tentativas + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            print(f"[Aviso] Tentativa {tentativa}/{max_tentativas} falhou com status {resp.status_code}. URL: {url}")
        except requests.RequestException as e:
            print(f"[Aviso] Tentativa {tentativa}/{max_tentativas} falhou com exceção: {e}. URL: {url}")

        if tentativa < max_tentativas:
            tempo_espera = delays[tentativa - 1]
            print(f"Aguardando {tempo_espera}s antes da próxima tentativa...")
            time.sleep(tempo_espera)

    raise RuntimeError(f"Falha ao consultar a URL após {max_tentativas} tentativas: {url}")


def salvar_json_raw(nome_prefixo, dados_json):
    """Salva a resposta original da API na pasta raw com data e hora no nome."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = os.path.join(RAW_DIR, f"{nome_prefixo}_{timestamp}.json")
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados_json, f, ensure_ascii=False, indent=2)
    return caminho


def extrair_municipios(url_custom=None):
    """
    Parte 1.2: Extrai municípios de Sergipe do IBGE.
    Usa pd.json_normalize para achatar o JSON aninhado e retorna todas as colunas em texto.
    """
    url = url_custom or "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SE/municipios"
    resp = requisicao_com_retry(url)
    dados = resp.json()
    salvar_json_raw("ibge_municipios", dados)
    df = pd.json_normalize(dados)
    return df.astype(str), url


def extrair_bcb(serie, data_inicial="01/01/2026", data_final="30/09/2026"):
    """
    Parte 1.2: Extrai série do Banco Central (1: Dólar, 432: Selic).
    Retorna DataFrame com todas as colunas em texto.
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
    params = {
        "formato": "json",
        "dataInicial": data_inicial,
        "dataFinal": data_final
    }
    resp = requisicao_com_retry(url, params=params)
    dados = resp.json()
    prefixo = "bcb_dolar_serie_1" if serie == 1 else f"bcb_serie_{serie}"
    salvar_json_raw(prefixo, dados)
    df = pd.DataFrame(dados)
    return df.astype(str), resp.url


def extrair_clima(latitude=-10.91, longitude=-37.07, nome_cidade="aracaju"):
    """
    Parte 1.2: Extrai temperatura e chuva no Open-Meteo diário com timezone America/Sao_Paulo.
    Retorna DataFrame da chave 'daily' com todas as colunas em texto.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "2026-01-01",
        "end_date": "2026-09-30",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "America/Sao_Paulo"
    }
    resp = requisicao_com_retry(url, params=params)
    dados = resp.json()
    salvar_json_raw(f"open_meteo_clima_{nome_cidade}", dados)
    daily = dados.get("daily", {})
    df = pd.DataFrame(daily)
    return df.astype(str), resp.url


def extrair_feriados():
    """
    Parte 1.2: Extrai feriados nacionais de 2026 na BrasilAPI.
    Retorna DataFrame com todas as colunas em texto.
    """
    url = "https://brasilapi.com.br/api/feriados/v1/2026"
    resp = requisicao_com_retry(url)
    dados = resp.json()
    salvar_json_raw("brasilapi_feriados_2026", dados)
    df = pd.DataFrame(dados)
    return df.astype(str), url


# ==============================================================================
# PARTE 2: Staging
# ==============================================================================
def salvar_staging(conn, df, nome_tabela, id_exec):
    """
    Parte 2.1: Função de staging.
    Recebe o DataFrame, nome da tabela stg_ e id_exec.
    Acrescenta id_exec e dt_carga, substitui pontos e hífens por sublinhado
    e grava no banco SQLite lab_etl.db substituindo a anterior.
    """
    df_stg = df.copy()
    df_stg["id_exec"] = id_exec
    df_stg["dt_carga"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Substituir pontos e hífens nos nomes das colunas
    novas_colunas = [c.replace(".", "_").replace("-", "_") for c in df_stg.columns]
    df_stg.columns = novas_colunas

    # Garantir que todo dado seja texto na staging
    df_stg = df_stg.astype(str)

    df_stg.to_sql(nome_tabela, conn, if_exists="replace", index=False)
    return len(df_stg)


# ==============================================================================
# PARTE 3: Transformação
# ==============================================================================
def transformar_municipios(conn):
    """
    Parte 3.2: Transforma dados de stg_municipio.
    Devolve: cod_ibge inteiro, nome, microrregiao, mesorregiao e uf.
    """
    df = pd.read_sql("SELECT * FROM stg_municipio", conn)
    df_transf = pd.DataFrame({
        "cod_ibge": df["id"].astype(int),
        "nome": df["nome"].astype(str),
        "microrregiao": df["microrregiao_nome"].astype(str),
        "mesorregiao": df["microrregiao_mesorregiao_nome"].astype(str),
        "uf": df["microrregiao_mesorregiao_UF_sigla"].astype(str)
    })
    return df_transf


def transformar_bcb(tabela_stg, conn):
    """
    Parte 3.2: Transforma séries do BCB (stg_dolar e stg_selic).
    Converte data de dd/mm/aaaa para AAAA-MM-DD e valor para numérico.
    """
    df = pd.read_sql(f"SELECT data, valor FROM {tabela_stg}", conn)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df


def classificar_chuva(chuva_mm):
    """
    Parte 3.2: Regra de classificação para a chuva diária:
    - sem dado se o valor for vazio (nulo)
    - sem chuva se for 0
    - fraca abaixo de 5 mm
    - moderada de 5 mm até menos de 20 mm
    - forte a partir de 20 mm
    """
    if pd.isna(chuva_mm):
        return "sem dado"
    if chuva_mm == 0:
        return "sem chuva"
    if chuva_mm < 5.0:
        return "fraca"
    if chuva_mm < 20.0:
        return "moderada"
    return "forte"


def transformar_clima(conn, cod_ibge=2800308, tabela_stg="stg_clima"):
    """
    Parte 3.2: Transforma dados de clima.
    Devolve: data, cod_ibge, temp_max, temp_min, chuva_mm e classe_chuva.
    """
    df = pd.read_sql(f"SELECT * FROM {tabela_stg}", conn)
    df_transf = pd.DataFrame()
    df_transf["data"] = df["time"].astype(str)
    df_transf["cod_ibge"] = int(cod_ibge)
    df_transf["temp_max"] = pd.to_numeric(df["temperature_2m_max"], errors="coerce")
    df_transf["temp_min"] = pd.to_numeric(df["temperature_2m_min"], errors="coerce")
    df_transf["chuva_mm"] = pd.to_numeric(df["precipitation_sum"], errors="coerce")
    df_transf["classe_chuva"] = df_transf["chuva_mm"].apply(classificar_chuva)
    return df_transf


def montar_dim_data(conn, inicio="2026-01-01", fim="2026-09-30"):
    """
    Parte 3.2: Monta a dimensão dim_data com calendário de 273 dias,
    ano, mes, dia, dia_semana, fim_de_semana (0 ou 1), feriado (0 ou 1) e nome_feriado.
    """
    datas = pd.date_range(start=inicio, end=fim, freq="D")
    df_data = pd.DataFrame({
        "data": datas.strftime("%Y-%m-%d"),
        "ano": datas.year,
        "mes": datas.month,
        "dia": datas.day,
        "dia_semana": datas.dayofweek,  # 0=Segunda, ..., 6=Domingo
        "fim_de_semana": datas.dayofweek.isin([5, 6]).astype(int)
    })

    # Cruzar com feriados nacionais de stg_feriado
    df_feriados = pd.read_sql("SELECT date, name FROM stg_feriado", conn)
    df_feriados = df_feriados.rename(columns={"date": "data", "name": "nome_feriado"})

    df_final = df_data.merge(df_feriados, on="data", how="left")
    df_final["feriado"] = df_final["nome_feriado"].notna().astype(int)
    df_final["nome_feriado"] = df_final["nome_feriado"].fillna(None)

    colunas = ["data", "ano", "mes", "dia", "dia_semana", "fim_de_semana", "feriado", "nome_feriado"]
    return df_final[colunas]


def montar_fato_indicador(conn, df_dim_data):
    """
    Parte 3.2: Monta fato_indicador_diario cruzando dólar, Selic e dim_data.
    Usa ffill para dólar_último_válido.
    """
    df_dolar = transformar_bcb("stg_dolar", conn).rename(columns={"valor": "dolar_venda"})
    df_selic = transformar_bcb("stg_selic", conn).rename(columns={"valor": "selic_meta"})

    # Inicia com as 273 datas
    df_indicador = df_dim_data[["data"]].copy()

    # Left join com Dólar
    df_indicador = df_indicador.merge(df_dolar, on="data", how="left")
    # Preenchimento adiante (ffill) para dias sem cotação
    df_indicador["dolar_ultimo_valido"] = df_indicador["dolar_venda"].ffill().bfill()

    # Left join com Selic
    df_indicador = df_indicador.merge(df_selic, on="data", how="left")
    df_indicador["selic_meta"] = df_indicador["selic_meta"].ffill().bfill()

    return df_indicador[["data", "dolar_venda", "dolar_ultimo_valido", "selic_meta"]]


# ==============================================================================
# PARTE 4: Carga e Esquema do Banco
# ==============================================================================
def criar_esquema(conn):
    """
    Parte 4.1: Cria o esquema do banco com as tabelas finais do Quadro 4,
    garantindo chaves primárias e chaves estrangeiras ativadas.
    """
    cursor = conn.cursor()
    # Ativa integridade de chaves estrangeiras no SQLite
    cursor.execute("PRAGMA foreign_keys = ON;")

    cursor.executescript("""
    -- Tabela de Controle
    CREATE TABLE IF NOT EXISTS etl_controle (
        id_exec INTEGER PRIMARY KEY,
        fonte TEXT NOT NULL,
        url TEXT,
        inicio TEXT NOT NULL,
        fim TEXT,
        status TEXT,
        linhas_lidas INTEGER,
        linhas_carregadas INTEGER,
        mensagem TEXT
    );

    -- Dimensões
    CREATE TABLE IF NOT EXISTS dim_municipio (
        cod_ibge INTEGER PRIMARY KEY,
        nome TEXT NOT NULL,
        microrregiao TEXT,
        mesorregiao TEXT,
        uf TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_data (
        data TEXT PRIMARY KEY,
        ano INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        dia INTEGER NOT NULL,
        dia_semana INTEGER NOT NULL,
        fim_de_semana INTEGER NOT NULL,
        feriado INTEGER NOT NULL,
        nome_feriado TEXT
    );

    -- Fatos
    CREATE TABLE IF NOT EXISTS fato_clima_diario (
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

    CREATE TABLE IF NOT EXISTS fato_indicador_diario (
        data TEXT PRIMARY KEY,
        dolar_venda REAL,
        dolar_ultimo_valido REAL,
        selic_meta REAL,
        FOREIGN KEY (data) REFERENCES dim_data(data)
    );

    -- Tabela Final da Parte 7: Fonte Livre (Clima de Estância - Litoral Sul)
    CREATE TABLE IF NOT EXISTS fato_clima_estancia (
        data TEXT PRIMARY KEY,
        cod_ibge INTEGER NOT NULL,
        temp_max REAL,
        temp_min REAL,
        chuva_mm REAL,
        classe_chuva TEXT,
        FOREIGN KEY (data) REFERENCES dim_data(data),
        FOREIGN KEY (cod_ibge) REFERENCES dim_municipio(cod_ibge)
    );
    """)
    conn.commit()


def carregar_tabelas_finais(conn, df_municipio, df_dim_data, df_fato_clima, df_fato_indicador, df_fato_estancia=None):
    """
    Parte 4.3: Realiza a carga idempotente das tabelas finais respeitando
    estritamente a ordem de integridade referencial:
    1º Dimensões (dim_municipio, dim_data)
    2º Fatos (fato_clima_diario, fato_indicador_diario, fato_clima_estancia)
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Limpeza ordenada para garantir idempotência sem violação de FK
    cursor.execute("DELETE FROM fato_clima_estancia;")
    cursor.execute("DELETE FROM fato_clima_diario;")
    cursor.execute("DELETE FROM fato_indicador_diario;")
    cursor.execute("DELETE FROM dim_data;")
    cursor.execute("DELETE FROM dim_municipio;")
    conn.commit()

    # Carga 1: Dimensões
    df_municipio.to_sql("dim_municipio", conn, if_exists="append", index=False)
    df_dim_data.to_sql("dim_data", conn, if_exists="append", index=False)

    # Carga 2: Fatos
    df_fato_clima.to_sql("fato_clima_diario", conn, if_exists="append", index=False)
    df_fato_indicador.to_sql("fato_indicador_diario", conn, if_exists="append", index=False)

    if df_fato_estancia is not None:
        df_fato_estancia.to_sql("fato_clima_estancia", conn, if_exists="append", index=False)

    conn.commit()


# ==============================================================================
# PARTE 5: Controle e Qualidade dos Dados
# ==============================================================================
def validar_qualidade(df_dim_data, df_fato_clima, df_fato_indicador):
    """
    Parte 5.1: Função de validação com regras de qualidade.
    Retorna a lista de problemas encontrados.
    """
    problemas = []

    # Regra 1: Quantidade esperada de 273 dias no calendário
    if len(df_dim_data) != 273:
        problemas.append(f"Regra 1 violada: dim_data tem {len(df_dim_data)} linhas, esperado 273.")

    # Regra 2: Precipitação não pode ser negativa
    chuva_negativa = df_fato_clima[df_fato_clima["chuva_mm"] < 0]
    if len(chuva_negativa) > 0:
        problemas.append(f"Regra 2 violada: fato_clima_diario possui {len(chuva_negativa)} valores de chuva < 0.")

    # Regra 3: Consistência térmica (temp_min <= temp_max)
    temp_invalida = df_fato_clima[df_fato_clima["temp_min"] > df_fato_clima["temp_max"]]
    if len(temp_invalida) > 0:
        problemas.append(f"Regra 3 violada: fato_clima_diario possui {len(temp_invalida)} dias com temp_min > temp_max.")

    # Regra 4: Ausência de duplicatas em chaves primárias
    if df_dim_data["data"].duplicated().any():
        problemas.append("Regra 4 violada: dim_data possui datas duplicadas.")

    if df_fato_clima.duplicated(subset=["data", "cod_ibge"]).any():
        problemas.append("Regra 4 violada: fato_clima_diario possui chave composta (data, cod_ibge) duplicada.")

    # Regra 5: Taxa Selic positiva
    if (df_fato_indicador["selic_meta"] <= 0).any():
        problemas.append("Regra 5 violada: Selic com valores menores ou iguais a zero.")

    return problemas


def registrar_controle(conn, id_exec, fonte, url, inicio, fim, status, linhas_lidas, linhas_carregadas, mensagem):
    """Parte 5.2: Registra ou atualiza execução na tabela etl_controle."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO etl_controle 
        (id_exec, fonte, url, inicio, fim, status, linhas_lidas, linhas_carregadas, mensagem)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (id_exec, fonte, url, inicio, fim, status, linhas_lidas, linhas_carregadas, mensagem))
    conn.commit()


# ==============================================================================
# PARTE 6: Consultas Respondendo ao Diretor
# ==============================================================================
def responder_perguntas_diretor(conn):
    """
    Parte 6: Executa as 5 consultas SQL para responder às perguntas do diretor.
    """
    perguntas = [
        (
            "P1: Em quais meses choveu forte em Aracaju e quantos dias foram em cada mês?",
            """
            SELECT 
                d.ano,
                d.mes,
                COUNT(*) AS dias_chuva_forte,
                ROUND(SUM(c.chuva_mm), 2) AS total_chuva_mm
            FROM fato_clima_diario c
            JOIN dim_data d ON c.data = d.data
            WHERE c.classe_chuva = 'forte'
            GROUP BY d.ano, d.mes
            ORDER BY d.mes;
            """
        ),
        (
            "P2: Como estava o tempo nos feriados nacionais do período?",
            """
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
            """
        ),
        (
            "P3: Qual foi o dólar médio de cada mês e quanto choveu no mesmo mês?",
            """
            SELECT 
                d.ano,
                d.mes,
                ROUND(AVG(i.dolar_venda), 4) AS dolar_medio_dias_uteis,
                ROUND(AVG(i.dolar_ultimo_valido), 4) AS dolar_medio_geral,
                ROUND(SUM(c.chuva_mm), 2) AS chuva_total_mes_mm
            FROM dim_data d
            JOIN fato_clima_diario c ON d.data = c.data
            JOIN fato_indicador_diario i ON d.data = i.data
            GROUP BY d.ano, d.mes
            ORDER BY d.mes;
            """
        ),
        (
            "P4: Em quantos dias a Selic mudou e quais foram a menor e a maior taxa do período?",
            """
            WITH mudancas AS (
                SELECT 
                    data,
                    selic_meta,
                    LAG(selic_meta) OVER (ORDER BY data) AS selic_anterior
                FROM fato_indicador_diario
            )
            SELECT 
                ROUND(MIN(selic_meta), 2) AS menor_selic,
                ROUND(MAX(selic_meta), 2) AS maior_selic,
                COUNT(CASE WHEN selic_meta != selic_anterior THEN 1 END) AS qtd_mudancas
            FROM mudancas;
            """
        ),
        (
            "P5: Quantos municípios cada mesorregião de Sergipe tem?",
            """
            SELECT 
                mesorregiao,
                COUNT(*) AS total_municipios
            FROM dim_municipio
            GROUP BY mesorregiao
            ORDER BY total_municipios DESC;
            """
        )
    ]

    print("\n=================================================================")
    print("      PARTE 6: CONSULTAS SQL RESPONDENDO AO DIRETOR")
    print("=================================================================")
    for titulo, sql in perguntas:
        print(f"\n--- {titulo} ---")
        df_res = pd.read_sql(sql, conn)
        print(df_res.to_string(index=False))


# ==============================================================================
# PARTE 7: Fonte Livre da Equipe (Clima de Estância - Litoral Sul de Sergipe)
# ==============================================================================
def executar_fonte_livre(conn, id_exec):
    """
    Parte 7: Extração, staging, transformação e carga da fonte livre escolhida:
    Clima histórico de Estância/SE (Latitude -11.26, Longitude -37.43, IBGE 2802106).
    """
    inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fonte = "Open-Meteo Clima Estância (Litoral Sul)"
    url = ""
    try:
        # Extração
        df_ext, url = extrair_clima(latitude=-11.26, longitude=-37.43, nome_cidade="estancia")
        lidas = len(df_ext)

        # Staging
        salvar_staging(conn, df_ext, "stg_clima_estancia", id_exec)

        # Transformação (Duas regras aplicadas: conversão numérica com classificação de chuva e associação do cod_ibge)
        df_transf = transformar_clima(conn, cod_ibge=2802106, tabela_stg="stg_clima_estancia")

        # Carga
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fato_clima_estancia;")
        df_transf.to_sql("fato_clima_estancia", conn, if_exists="append", index=False)
        conn.commit()

        fim = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        carregadas = len(df_transf)
        registrar_controle(conn, id_exec, fonte, url, inicio, fim, "SUCESSO", lidas, carregadas, "Fonte livre carregada com sucesso.")
        return df_transf
    except Exception as e:
        fim = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        registrar_controle(conn, id_exec, fonte, url, inicio, fim, "ERRO", 0, 0, str(e))
        print(f"Erro na fonte livre: {e}")
        return None


def consulta_negocio_fonte_livre(conn):
    """Consulta de negócio para a Fonte Livre."""
    sql = """
    SELECT 
        d.data,
        CASE d.dia_semana WHEN 5 THEN 'Sábado' WHEN 6 THEN 'Domingo' END AS dia_fim_semana,
        e.temp_max,
        e.chuva_mm,
        e.classe_chuva
    FROM fato_clima_estancia e
    JOIN dim_data d ON e.data = d.data
    WHERE d.fim_de_semana = 1 
      AND e.chuva_mm = 0 
      AND e.temp_max >= 29.0
    ORDER BY d.data
    LIMIT 10;
    """
    print("\n--- Parte 7: Consulta de Negócio da Fonte Livre ---")
    print("Pergunta: Quais foram os melhores fins de semana com sol (sem chuva e temp_max >= 29°C) no litoral sul (Estância/Praia do Saco) para ofertar pacotes?")
    df = pd.read_sql(sql, conn)
    print(df.to_string(index=False))


# ==============================================================================
# PIPELINE ETL PRINCIPAL
# ==============================================================================
def executar_pipeline(simular_erro_ibge=False):
    """
    Executa o processo completo de ETL:
    - Extração das 5 fontes + fonte livre
    - Staging de todas as fontes
    - Transformações e criação de dimensões e fatos
    - Validação de qualidade de dados
    - Carga no banco SQLite com foreign keys ativadas
    - Registro detalhado na etl_controle
    """
    print(f"\nIniciando Pipeline ETL Costa em Dados [Banco: {DB_PATH}]...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    criar_esquema(conn)

    # 1. Extrações e Staging
    fontes = [
        (1, "IBGE Localidades", extrair_municipios if not simular_erro_ibge else lambda: extrair_municipios("https://servicodados.ibge.gov.br/api/v1/localidades_inexistente/SE"), "stg_municipio"),
        (2, "BCB Dólar (Série 1)", lambda: extrair_bcb(1), "stg_dolar"),
        (3, "BCB Selic (Série 432)", lambda: extrair_bcb(432), "stg_selic"),
        (4, "Open-Meteo Aracaju Clima", extrair_clima, "stg_clima"),
        (5, "BrasilAPI Feriados", extrair_feriados, "stg_feriado")
    ]

    contagens_staging = {}

    for id_exec, nome_fonte, func_extrair, nome_stg in fontes:
        inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        url_usada = ""
        try:
            print(f"-> Extraindo {nome_fonte}...")
            df_extraido, url_usada = func_extrair()
            linhas_lidas = len(df_extraido)

            print(f"   Salvando em staging: {nome_stg}...")
            salvar_staging(conn, df_extraido, nome_stg, id_exec)
            contagens_staging[nome_fonte] = linhas_lidas

            fim = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            registrar_controle(conn, id_exec, nome_fonte, url_usada, inicio, fim, "SUCESSO", linhas_lidas, linhas_lidas, "Extração e staging concluídos.")
        except Exception as e:
            fim = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            registrar_controle(conn, id_exec, nome_fonte, url_usada, inicio, fim, "ERRO", 0, 0, str(e))
            print(f"   [FALHA] Erro em {nome_fonte}: {e}")
            if simular_erro_ibge and id_exec == 1:
                print("   [INFO] Erro de teste simulado com sucesso na etl_controle.")
                conn.close()
                return

    # 2. Transformações
    print("\n-> Executando Transformações de Dados...")
    df_municipio = transformar_municipios(conn)
    df_dim_data = montar_dim_data(conn)
    df_fato_clima = transformar_clima(conn, cod_ibge=2800308, tabela_stg="stg_clima")
    df_fato_indicador = montar_fato_indicador(conn, df_dim_data)

    # 3. Validação de Qualidade
    print("-> Verificando Regras de Qualidade dos Dados...")
    problemas = validar_qualidade(df_dim_data, df_fato_clima, df_fato_indicador)
    if problemas:
        print("[ALERTA] Problemas de qualidade detectados:")
        for p in problemas:
            print(f" - {p}")
    else:
        print("   Todas as regras de qualidade foram satisfeitas com sucesso!")

    # 4. Carga Final Idempotente
    print("-> Realizando Carga nas Tabelas Finais (Dimensões e Fatos)...")
    carregar_tabelas_finais(conn, df_municipio, df_dim_data, df_fato_clima, df_fato_indicador)

    # Atualiza registro de linhas carregadas nas fontes correspondentes
    cursor = conn.cursor()
    cursor.execute("UPDATE etl_controle SET linhas_carregadas = ? WHERE id_exec = 1", (len(df_municipio),))
    # Para o Dólar, as 273 linhas foram geradas na fato_indicador através do calendário
    cursor.execute("UPDATE etl_controle SET linhas_carregadas = ? WHERE id_exec = 2", (len(df_fato_indicador),))
    cursor.execute("UPDATE etl_controle SET linhas_carregadas = ? WHERE id_exec = 3", (len(df_fato_indicador),))
    cursor.execute("UPDATE etl_controle SET linhas_carregadas = ? WHERE id_exec = 4", (len(df_fato_clima),))
    cursor.execute("UPDATE etl_controle SET linhas_carregadas = ? WHERE id_exec = 5", (len(df_dim_data[df_dim_data['feriado'] == 1]),))
    conn.commit()

    # 5. Fonte Livre (Parte 7)
    print("-> Executando Fonte Livre (Parte 7 - Clima Estância)...")
    executar_fonte_livre(conn, id_exec=6)

    print("\n-> Pipeline Concluído com Sucesso!")

    # Exibição dos Relatórios e Respostas
    exibir_status_tabelas(conn)
    responder_perguntas_diretor(conn)
    consulta_negocio_fonte_livre(conn)

    conn.close()


def exibir_status_tabelas(conn):
    """Exibe a contagem de linhas em todas as tabelas do banco."""
    print("\n=================================================================")
    print("               CONTAGEM DE LINHAS NO BANCO FINAL")
    print("=================================================================")
    tabelas = [
        "stg_municipio", "stg_dolar", "stg_selic", "stg_clima", "stg_feriado", "stg_clima_estancia",
        "dim_municipio", "dim_data", "fato_clima_diario", "fato_indicador_diario", "fato_clima_estancia", "etl_controle"
    ]
    cursor = conn.cursor()
    for t in tabelas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            qtd = cursor.fetchone()[0]
            print(f"Tabela: {t:<22} | Linhas: {qtd}")
        except Exception as e:
            print(f"Tabela: {t:<22} | Não encontrada ({e})")

    print("\n--- Registro na Tabela etl_controle ---")
    df_ctrl = pd.read_sql("SELECT id_exec, fonte, status, linhas_lidas, linhas_carregadas, mensagem FROM etl_controle", conn)
    print(df_ctrl.to_string(index=False))


if __name__ == "__main__":
    # Teste curto inicial da Parte 0
    teste_inicial_bcb()

    # Execução do Pipeline Completo
    executar_pipeline()
