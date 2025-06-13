# Code Elevate Assignment - Diario de Bordo

A amostra de dados em anexo (`info_transportes.csv`) possui dados de um aplicativo de transporte privado, cujas colunas são:

- `DATA_INICIO` (formato: `"mm-dd-yyyy HH"`)
- `DATA_FIM` (formato: `"mm-dd-yyyy HH"`)
- `CATEGORIA`
- `LOCAL_INICIO`
- `LOCAL_FIM`
- `PROPOSITO`
- `DISTANCIA`

Uma equipe está elaborando um modelo para compreender como os clientes estão utilizando o aplicativo. Para isso, você precisa fornecer uma nova tabela chamada **`info_corridas_do_dia`**, com dados agrupados pela data de início do transporte utilizando a formatação `"yyyy-MM-dd"`, contendo as seguintes colunas:

| Nome da Coluna        | Descrição                                                                 |
|-----------------------|---------------------------------------------------------------------------|
| `DT_REFE`             | Data de referência.                                                       |
| `QT_CORR`             | Quantidade de corridas.                                                   |
| `QT_CORR_NEG`         | Quantidade de corridas com a categoria “Negócio”.                         |
| `QT_CORR_PESS`        | Quantidade de corridas com a categoria “Pessoal”.                         |
| `VL_MAX_DIST`         | Maior distância percorrida por uma corrida.                               |
| `VL_MIN_DIST`         | Menor distância percorrida por uma corrida.                               |
| `VL_AVG_DIST`         | Média das distâncias percorridas.                                         |
| `QT_CORR_REUNI`       | Quantidade de corridas com o propósito de "Reunião".                      |
| `QT_CORR_NAO_REUNI`   | Quantidade de corridas com propósito declarado e diferente de "Reunião".  |

### Exemplo de linha da tabela:

| `DT_REFE`   | `QT_CORR` | `QT_CORR_NEG` | `QT_CORR_PESS` | `VL_MAX_DIST` | `VL_MIN_DIST` | `VL_AVG_DIST` | `QT_CORR_REUNI` | `QT_CORR_NAO_REUNI` |
|-------------|------------|----------------|------------------|----------------|----------------|----------------|-------------------|----------------------|
| 2022-01-01  | 20         | 12             | 8                | 2.2            | 0.7            | 1.1            | 6                 | 10                   |


## InfoTransportes Pipeline

Pipeline de ingestão, transformação e agregação de dados de transportes implementado com PySpark, Delta Lake e Docker. O projeto segue as melhores práticas de engenharia de dados com arquitetura em camadas (Bronze, Silver, Gold) e cobertura completa de testes unitários.

```mermaid
graph LR
    A[Input CSV] -->|Ingestão| B[Bronze Layer]
    B -->|Transformação| C[Silver Layer]
    C -->|Agregação| D[Gold Layer]
    D -->|Consulta| E[Notebook]
```

### Estrutura de Diretórios

```
assignment-diario-bordo/
├── src/                          # Código fonte do pipeline
│   ├── config.py                 # Configurações e constantes
│   ├── main.py                   # Ponto de entrada do pipeline
│   ├── data_tools/               # Módulos do pipeline de dados
│   │   ├── data_reader.py        # Classe para leitura de dados
│   │   ├── data_writer.py        # Classe para escrita de dados
│   │   ├── data_processor.py     # Orquestrador principal
│   │   └── utils/                # Utilitários e transformações
│   │       ├── config.py         # Configuração de dados
│   │       ├── exceptions.py     # Exceções customizadas
│   │       ├── processors.py     # Processadores Bronze/Silver/Gold
│   │       └── transformations.py # Transformações de dados
│   └── notebooks/                # Notebooks Jupyter para análise
│       └── explore_tables.ipynb  # Exploração das tabelas Delta
├── tests/                        # Testes unitários
│   ├── conftest.py              # Configurações e fixtures
│   ├── test_data_reader.py      # Testes do DataReader
│   ├── test_data_writer.py      # Testes do DataWriter
│   ├── test_transformations.py  # Testes das transformações
├── data/                        # Dados persistidos localmente
│   ├── input/                   # Dados de entrada (CSV)
│   ├── bronze/                  # Camada Bronze (dados brutos)
│   ├── silver/                  # Camada Silver (dados tratados)
│   └── gold/                    # Camada Gold (dados agregados)
├── spark-warehouse/             # Warehouse do Spark
├── derby/                       # Metastore Derby
├── docker-compose.yml           # Configuração Docker
├── Dockerfile                   # Imagem Docker
├── requirements.txt             # Dependências Python
└── pytest.ini                  # Configuração de testes
```

### Arquitetura de Dados

O pipeline implementa a arquitetura medallion com três camadas:

#### Bronze (Camada Bruta)
- **Fonte**: Arquivo CSV (`info_transportes.csv`)
- **Formato**: Delta Lake
- **Transformações**: Normalização de nomes de colunas (lowercase)
- **Validações**: Verificação de colunas obrigatórias

#### Silver (Camada Tratada)
- **Fonte**: Tabela Bronze
- **Formato**: Delta Lake particionado por data
- **Transformações**:
  - Conversão de datas (`data_inicio` → `dt_refe`)
  - Normalização de texto (lowercase para categorias)
  - Limpeza de dados nulos
- **Particionamento**: Por `dt_refe` (data de referência)

#### Gold (Camada Agregada)
- **Fonte**: Tabela Silver
- **Formato**: Delta Lake particionado por data
- **Agregações**:
  - Contagem total de corridas por dia
  - Contagem por categoria (negócio/pessoal)
  - Contagem por propósito (reunião/não-reunião)
  - Estatísticas de distância (min, max, média)


## Pré-requisitos

- **Docker** e **Docker Compose** instalados
- **Git** para clonar o repositório
- **Python 3.9+** (opcional, apenas para desenvolvimento local)

## Como Executar o Pipeline

### 1. Preparação dos Dados

Coloque o arquivo CSV na pasta de entrada:

```
data/input/info_transportes.csv
```

**Formato esperado do CSV:**
- Separador: ponto e vírgula (`;`)
- Header: obrigatório
- Colunas obrigatórias: `data_inicio`, `categoria`, `distancia`, `proposito`

### 2. Execução Completa

Execute o pipeline completo com Docker Compose:

```powershell
docker-compose up --build
```

Este comando irá:
1. Construir a imagem Docker com todas as dependências
2. Executar o pipeline completo: CSV → Bronze → Silver → Gold
3. Salvar os dados nas respectivas pastas locais

### 3. Verificação dos Resultados

Após a execução, verifique os dados gerados:

```powershell
# Verificar estrutura das pastas
ls data/bronze/
ls data/silver/
ls data/gold/

# Verificar logs do container
docker-compose logs
```

### 4. Limpeza

Para parar e remover os containers:

```powershell
docker-compose down
```

## Executando Testes Unitários

O projeto possui cobertura completa de testes unitários para todos os componentes.

### Execução dos Testes

1. **Subir o container:**
```powershell
docker-compose up -d
```

2. **Executar todos os testes:**
```powershell
docker exec -it spark_test_container bash -c "export PYTHONPATH=/app/src:/app:`$PYTHONPATH && cd /app && pytest tests/ -v"
```

## Exploração de Dados com Notebook

### Executar Jupyter Notebook

```powershell
docker-compose run --rm -p 8888:8888 spark-job jupyter notebook --ip 0.0.0.0 --allow-root --no-browser /app/src/notebooks
```

Acesse `http://localhost:8888` no navegador e abra `explore_tables.ipynb`.

### Funcionalidades do Notebook

- Configuração automática do Spark com Delta Lake
- Exploração das tabelas Bronze, Silver e Gold
- Visualização de schemas e dados
- Consultas SQL customizadas
- Análise de partições e metadados

## Configuração e Customização

### Variáveis de Ambiente

O projeto usa variáveis de ambiente definidas em `src/config.py`:

```python
# Nomes das tabelas
BRONZE_TABLE = "b_info_transportes"
SILVER_TABLE = "s_info_transportes" 
GOLD_TABLE = "info_corridas_do_dia"

# Caminhos dos dados
INPUT_PATH = "./data/input/info_transportes.csv"
BRONZE_PATH = "./data/bronze/b_info_transportes"
SILVER_PATH = "./data/silver/s_info_transportes"
GOLD_PATH = "./data/gold/info_corridas_do_dia"
```

### Exemplo de Arquivo .env

Crie um arquivo `.env` na raiz do projeto com as seguintes configurações:

```properties
# Caminhos base
BASE_PATH=./data
INPUT_PATH=./data/input/info_transportes.csv

# Nomes das tabelas
BRONZE_TABLE=b_info_transportes
SILVER_TABLE=s_info_transportes
GOLD_TABLE=info_corridas_do_dia

# Caminhos com nomes das tabelas
BRONZE_PATH=./data/bronze/${BRONZE_TABLE}
SILVER_PATH=./data/silver/${SILVER_TABLE}
GOLD_PATH=./data/gold/${GOLD_TABLE}
```

### Configuração do Spark

O Spark é configurado em `main.py` com:
- Delta Lake habilitado
- Metastore Derby para persistência
- Warehouse local para tabelas
- Configurações otimizadas para container

## Desenvolvimento e Extensão

### Adicionando Novas Transformações

1. Implemente a classe em `src/data_tools/utils/transformations.py`
2. Herde de `ColumnTransformation`
3. Implemente o método `apply()`
4. Adicione testes em `tests/test_transformations.py`

### Adicionando Novos Processadores

1. Crie a classe em `src/data_tools/utils/processors.py`
2. Herde de `BaseProcessor`
3. Implemente o método `process()`
4. Registre no `ProcessorFactory`
5. Adicione testes correspondentes

## Dependências Principais

- **PySpark 3.3.0+**: Motor de processamento distribuído
- **Delta Lake 2.1.0**: Formato de armazenamento com controle de versão
- **pytest 7.0.0+**: Framework de testes
- **python-dotenv**: Gerenciamento de variáveis de ambiente
- **Faker**: Geração de dados para testes

## Autor

**Rafael Passador**  
Code Elevate Assignment - 2025