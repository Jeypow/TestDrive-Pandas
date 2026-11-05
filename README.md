# Laboratório de Banco de Dados | Processo ETL na Prática
## Situação-Problema: Costa dos Coqueiros Turismo

Este repositório contém a solução completa para o **Laboratório de Processo ETL na Prática**, construído com Python (`pandas`, `requests`) e SQLite, integrando e tratando dados de APIs públicas para responder às necessidades estratégicas da agência **Costa dos Coqueiros Turismo** (Aracaju - Sergipe).

Arquivo base do laboratório: [Lab_ETL_Pratica_Costa_dos_Coqueiros 1 (1).pdf](https://github.com/user-attachments/files/34155429/Lab_ETL_Pratica_Costa_dos_Coqueiros.1.1.pdf)

---

## Uso de Inteligência Artificial no Projeto

> **Nota de Transparência e Desenvolvimento:**  
> Durante o diagnóstico preliminar das bases de dados brutas, **foi utilizada a assistência de Inteligência Artificial (IA)** como suporte analítico. A IA foi fundamental para **descobrir e evidenciar que os dados recebidos das APIs públicas estavam completamente heterogêneos e fora de padrão, exigindo um rigoroso processo de normalização e padronização**:
>
> 1. **Estruturas Hierárquicas Aninhadas:** Identificação de que o endpoint do IBGE entregava objetos JSON multinível (ex.: `microrregiao.mesorregiao.nome`), necessitando do achatamento relacional (*flattening*) com `pd.json_normalize`.
> 2. **Divergência de Formatos de Data e Tipos:** Constatação de que o Banco Central fornecia cotações como texto no formato brasileiro (`DD/MM/AAAA`), enquanto as APIs climáticas usavam o padrão ISO (`AAAA-MM-DD`), exigindo conversão uniforme para possibilitar relacionamentos entre tabelas.
> 3. **Lacunas Temporais nos Indicadores Financeiros:** Percepção de que o mercado de câmbio não opera aos finais de semana e feriados bancários (187 dias lidos vs 273 dias do período), o que quebraria cruzamentos com o clima. Com o auxílio da IA, estruturou-se a técnica de preenchimento adiante (*forward fill* / `ffill`) na coluna `dolar_ultimo_valido`.
> 4. **Modelagem Dimensional Idempotente:** Auxílio na definição do modelo dimensional (*Star Schema*) com chaves estrangeiras ativas e rotinas de carga idempotentes.

---

## Arquitetura do Pipeline ETL

O fluxo de dados foi construído seguindo as melhores práticas de Engenharia de Dados:

```
[ APIs Públicas ]
       │ (Chamada HTTP com retry exponencial: 1s, 2s, 4s)
       ▼
 [ Pasta raw/ ]  ────► Armazenamento imutável dos JSONs brutos com timestamp
       │
       ▼
[ Staging Area ] ────► Tabelas stg_* no SQLite (dados brutos como texto + id_exec + dt_carga)
       │
       ▼
[ Transformação ] ───► Normalização com pandas, tipagem, regras de chuva e dim_data (273 dias)
       │
       ▼
[ Banco Final ]  ────► SQLite (lab_etl.db) com PRAGMA foreign_keys = ON;
                       ├─ dim_municipio (75 municípios de SE)
                       ├─ dim_data (273 dias de 01/01 a 30/09/2026)
                       ├─ fato_clima_diario (273 dias de clima em Aracaju)
                       ├─ fato_indicador_diario (273 dias de Dólar e Selic)
                       ├─ fato_clima_estancia (Fonte Livre: Litoral Sul de Sergipe)
                       └─ etl_controle (Logs e auditoria de execução)
```

---

## Estrutura de Pastas e Arquivos

* **`lab_etl/lab_etl.py`**: Script principal contendo todas as funções de extração, staging, transformação, validação de qualidade, carga e consultas analíticas.
* **`lab_etl/lab_etl.db`**: Banco de dados relacional SQLite populado com as tabelas de staging, dimensões, fatos e controle.
* **`raw/`**: Diretório contendo os arquivos JSON brutos originais extraídos das APIs com carimbo de data e hora no nome.
* **`RESPOSTAS_LAB_ETL.md`**: Relatório técnico completo com todas as respostas, quadros preenchidos (Partes 0 a 7), logs e consultas SQL formatadas.
* **`respostas_lab_etl.tex`**: Código-fonte em LaTeX estruturado e diagramado, pronto para compilação do relatório final em formato PDF.

---

## Como Executar o Projeto

### Pré-requisitos
Certifique-se de ter o Python 3 instalado no seu ambiente. Instale as bibliotecas necessárias:
```powershell
pip install pandas requests
```

### 1. Executar o Pipeline ETL Completo
Na raiz do repositório, execute:
```powershell
python lab_etl/lab_etl.py
```
Esse comando irá:
1. Executar o teste preliminar de conectividade com a API do Banco Central (Parte 0).
2. Extrair dados das APIs públicas (IBGE, BCB Dólar, BCB Selic, Open-Meteo Aracaju, BrasilAPI e Open-Meteo Estância).
3. Salvar os arquivos originais na pasta `raw/`.
4. Ingerir os dados brutos nas tabelas `stg_*`.
5. Aplicar as transformações, padronizações e validações de qualidade de dados.
6. Carregar as tabelas dimensionais e de fatos no SQLite com chaves estrangeiras ativas.
7. Executar e exibir no console as respostas para as perguntas estratégicas do diretor.

### 2. Consultar Apenas as Perguntas do Diretor
Com o banco já gerado, você pode rodar apenas as consultas SQL sem refazer as requisições de rede:
```powershell
python -c "import sqlite3; from lab_etl.lab_etl import responder_perguntas_diretor; conn = sqlite3.connect('lab_etl/lab_etl.db'); responder_perguntas_diretor(conn); conn.close()"
```

---

##  Principais Descobertas de Negócio (Respondendo ao Diretor)

* **P1 (Chuva forte por mês):** Foram registrados apenas 2 dias de chuva forte ($\ge 20\text{ mm}$) em Aracaju no período: um dia em março (26,8 mm) e um em abril (33,6 mm).
* **P2 (Tempo nos feriados):** A grande maioria dos feriados nacionais teve tempo estável com chuva fraca e temperaturas de até 30°C. O único feriado com chuva forte foi Tiradentes (21/04).
* **P3 (Dólar vs Chuva):** O dólar médio oscilou entre R\$ 4,9907 (maio) e R\$ 5,3457 (janeiro). As chuvas concentraram-se entre abril e junho, com ápice em abril (168,0 mm).
* **P4 (Selic meta):** A taxa Selic variou de 13,75% a 15,00%, sofrendo alteração em 5 dias específicos no período.
* **P5 (Municípios de Sergipe):** O estado possui 75 municípios divididos em 3 mesorregiões: Leste Sergipano (42 municípios), Agreste Sergipano (18) e Sertão Sergipano (15).
* **P7 (Fonte Livre - Clima no Litoral Sul / Estância):** Identificação dos melhores fins de semana de sol e calor no litoral sul (Praia do Saco) para o lançamento de pacotes turísticos da agência.
