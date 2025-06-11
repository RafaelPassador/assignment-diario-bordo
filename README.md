# 🚛 InfoTransportes Pipeline

Pipeline de ingestão, transformação e agregação de dados de transportes usando PySpark e Docker.

## 📂 Organização

- `src/` - Código do pipeline
  - `data_reader.py`, `data_writer.py`, `data_processor.py` (modular)
- `data/` - Diretório montado localmente com:
  - `input/` → entrada em CSV
  - `bronze/` → dados brutos
  - `silver/` → dados tratados
  - `gold/` → dados agregados
- `.env` - Define os caminhos dinamicamente
- `ValidacaoCamadas.ipynb` - Notebook para validação dos dados
- `tests/` - Testes automatizados com `pytest`
- `docker/` - Infraestrutura Docker

## 🚀 Como Executar com Docker

### 1. Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.9+ instalado (apenas para rodar o notebook ou testar localmente)

### 2. Prepare a entrada

Coloque o arquivo CSV em:

```bash
C:\Users\Usuario\Documents\GitHub\assignment-diario-bordo\data\input\info_transportes.csv
```

> ❗ Crie a pasta `input/` se ela ainda não existir.  
> ❌ As pastas `bronze/`, `silver/` e `gold/` **não precisam existir** – o Spark criará essas automaticamente.

### 3. Execute o pipeline completo com:

```bash
docker-compose up --build
```

Isso irá:

- Rodar os testes automatizados
- Executar o pipeline: `CSV -> Bronze -> Silver -> Gold`

### 4. Verifique os dados salvos

As pastas serão criadas no seu computador em:

```
C:\Users\Usuario\Documents\GitHub\assignment-diario-bordo\data\bronze\
C:\Users\Usuario\Documents\GitHub\assignment-diario-bordo\data\silver\
C:\Users\Usuario\Documents\GitHub\assignment-diario-bordo\data\gold\
```

Você pode inspecioná-las com:

```bash
ls data/silver/
```

Ou usando o notebook.

## 📒 Validar dados com Notebook

Execute localmente:

```bash
jupyter notebook src/notebooks/explore_tables.ipynb

ou

docker-compose run --rm -p 8888:8888 spark-job jupyter notebook --ip 0.0.0.0 --allow-root --no-browser /app/src/notebooks    
```

Esse notebook irá:

- Inicializar Spark
- Ler dados das camadas `bronze`, `silver` e `gold`
- Mostrar o schema e primeiras linhas
- Explorar partições e realizar consultas personalizadas

## ✅ Testes automatizados

Os testes são executados automaticamente com `pytest`:

- Leitura e escrita
- Falhas simuladas
- Transformações Silver e Gold

Para rodar localmente:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Para rodar via Docker:

```bash
docker-compose run --rm spark-job python3 -m pytest tests/ -v
```

## ⚙️ CI com GitHub Actions

- Roda os testes a cada push/pull request na branch `main`.

## 🧠 Notas Técnicas

- Particionado por `DT_REFE`
- Dados organizados em camadas Bronze, Silver e Gold
- Docker cria os diretórios e salva os dados localmente no host
- Estrutura genérica, extensível e escalável

## 🛠️ Autor

Rafael Passador · Engenharia de Dados · 2025