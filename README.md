# Apache Spark com Delta Lake e Apache Iceberg

> **Trabalho Acadêmico** — Pós-Graduação em Engenharia de Dados

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-003366)](https://delta.io/)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.6.1-4285F4)](https://iceberg.apache.org/)
[![UV](https://img.shields.io/badge/UV-latest-7c3aed)](https://docs.astral.sh/uv/)
[![MkDocs](https://img.shields.io/badge/MkDocs-Material-526CFE?logo=materialformkdocs)](https://squidfunk.github.io/mkdocs-material/)

---

## 👥 Alunos

| Nome | RA |
|------|----|
| [Nome do Aluno 1] | [RA] |
| [Nome do Aluno 2] | [RA] |

**Professor:** Prof. Jorge Lima da Silva

---

## 🎯 Tema

Demonstração prática dos formatos de tabela abertos **Delta Lake** e **Apache Iceberg** integrados
ao **Apache Spark (PySpark)**, utilizando o cenário de um **Marketplace de Máquinas Pesadas**.

### Repositórios de Referência

- [spark-delta](https://github.com/jlsilva01/spark-delta) — Prof. Jorge Lima da Silva
- [spark-iceberg](https://github.com/jlsilva01/spark-iceberg) — Prof. Jorge Lima da Silva

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Versão | Papel |
|------------|--------|-------|
| **Python** | 3.11 | Linguagem de programação |
| **Apache Spark / PySpark** | 3.5.3 | Motor de processamento distribuído |
| **Delta Lake** | 3.2.0 | Formato de tabela ACID sobre Parquet |
| **Apache Iceberg** | 1.6.1 | Formato de tabela aberto com snapshots |
| **JupyterLab** | 4.x | Ambiente interativo de notebooks |
| **UV** | latest | Gerenciador de pacotes e ambiente Python |
| **Java** | 17 | Requisito obrigatório do Apache Spark |
| **MkDocs Material** | 9.x | Geração de documentação estática |

---

## 📁 Estrutura do Projeto

```
spark-delta-iceberg-trabalho/
├── README.md                    ← Este arquivo
├── pyproject.toml               ← Dependências Python (UV)
├── .python-version              ← Python 3.11
├── .gitignore
├── mkdocs.yml                   ← Configuração da documentação
├── notebooks/
│   ├── 01_delta_lake.ipynb     ← Notebook: Delta Lake
│   └── 02_iceberg.ipynb        ← Notebook: Apache Iceberg
├── docs/
│   ├── index.md                ← Contextualização do trabalho
│   ├── spark.md                ← Apache Spark e PySpark
│   ├── delta.md                ← Delta Lake
│   └── iceberg.md              ← Apache Iceberg
├── data/                        ← Dados auxiliares
└── assets/                      ← Imagens e recursos
```

---

## ⚙️ Pré-requisitos do Sistema

### 1. Java 17 (obrigatório para Apache Spark)

O Apache Spark **exige Java** instalado e configurado no sistema.

#### Windows

```powershell
# Verificar se Java está instalado
java -version
```

Se não estiver instalado, baixe o **Adoptium Temurin 17** em:
👉 https://adoptium.net/temurin/releases/?version=17

Após instalar, configure as variáveis de ambiente (PowerShell):

```powershell
# Substituir pelo seu caminho de instalação real
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Para tornar permanente, adicione via **Painel de Controle → Sistema → Variáveis de Ambiente**.

#### Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install openjdk-17-jdk
java -version
```

#### macOS

```bash
brew install openjdk@17
export JAVA_HOME=$(brew --prefix openjdk@17)
export PATH=$JAVA_HOME/bin:$PATH
```

---

### 2. UV — Gerenciador de Pacotes Python

O **UV** é um gerenciador de pacotes e ambientes virtuais ultrarrápido para Python.

#### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Linux / macOS

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Verificar instalação

```bash
uv --version
# Saída esperada: uv 0.x.x (...)
```

---

## 🚀 Reproduzindo o Ambiente (Passo a Passo)

### Passo 1 — Clonar o repositório

```bash
git clone https://github.com/seu-usuario/spark-delta-iceberg-trabalho.git
cd spark-delta-iceberg-trabalho
```

### Passo 2 — Criar ambiente virtual e instalar dependências

```bash
uv sync
```

O UV irá:
- Ler o arquivo `.python-version` e usar Python 3.11
- Criar o ambiente virtual em `.venv/`
- Instalar todas as dependências do `pyproject.toml` (PySpark, Delta Lake, JupyterLab, MkDocs...)

> **⏱️ Tempo estimado:** 2–5 minutos na primeira execução (download dos pacotes).

### Passo 3 — Registrar o kernel do Jupyter

Para que o JupyterLab reconheça o ambiente com todas as dependências:

```bash
uv run python -m ipykernel install --user --name spark-delta-iceberg --display-name "Python (Spark Delta Iceberg)"
```

### Passo 4 — Abrir no VS Code

```bash
code .
```

> No VS Code, ao abrir um notebook `.ipynb`, clique em **"Select Kernel"** no canto superior direito
> e escolha **"Python (Spark Delta Iceberg)"**.

### Passo 5 — Ou abrir o JupyterLab no navegador

```bash
uv run jupyter lab
```

Acesse: [http://localhost:8888](http://localhost:8888)

---

## 📓 Executando os Notebooks

### ⚠️ Importante: Execute um notebook por vez

Cada notebook cria uma `SparkSession` que usa memória significativa. Não execute os dois ao mesmo tempo.

### Notebook 1 — Delta Lake

Abra `notebooks/01_delta_lake.ipynb` e execute as células em ordem (**Shift+Enter** ou **Run All**).

**O notebook irá:**
1. ⏳ Baixar os JARs do Delta Lake do Maven (~30–60s na primeira execução)
2. ✅ Criar a `SparkSession` com extensões Delta
3. ✅ Criar a tabela `maquinas_delta` em `spark-warehouse/delta/`
4. ✅ Executar INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT → DESCRIBE HISTORY

### Notebook 2 — Apache Iceberg

Abra `notebooks/02_iceberg.ipynb` e execute as células em ordem.

**O notebook irá:**
1. ⏳ Baixar os JARs do Iceberg do Maven (~30–60s na primeira execução)
2. ✅ Criar a `SparkSession` com catálogo Hadoop local
3. ✅ Criar o database `local.trabalho` e a tabela `maquinas_iceberg`
4. ✅ Executar INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT → snapshots

---

## 📚 Documentação (MkDocs)

### Visualizar localmente

```bash
uv run mkdocs serve
```

Acesse: [http://localhost:8000](http://localhost:8000)

A documentação é atualizada automaticamente ao salvar qualquer arquivo em `docs/`.

### Gerar build estático

```bash
uv run mkdocs build
```

Os arquivos HTML são gerados no diretório `site/` (ignorado pelo `.gitignore`).

### Publicar no GitHub Pages

```bash
uv run mkdocs gh-deploy
```

> **Pré-requisitos para gh-deploy:**
> 1. O repositório deve estar no GitHub
> 2. O remote `origin` deve estar configurado (`git remote -v`)
> 3. Você deve ter permissão de push no repositório
> 4. Habilitar GitHub Pages nas configurações: **Settings → Pages → Source: gh-pages**

A documentação ficará disponível em:
`https://seu-usuario.github.io/spark-delta-iceberg-trabalho/`

---

## ⚠️ Erros Comuns e Soluções

### ❌ `JAVA_HOME is not set` ou `Java not found`

```
Error: JAVA_HOME is not set or Java not found in PATH
```

**Solução:** Instale o Java 17 e configure `JAVA_HOME` (ver Pré-requisitos acima).

```bash
# Verificar
java -version
echo $JAVA_HOME      # Linux/macOS
echo $env:JAVA_HOME  # Windows PowerShell
```

---

### ❌ `py4j.protocol.Py4JJavaError` ao iniciar SparkSession

Indica problema com Java. Verifique a versão e o `JAVA_HOME`.

```bash
java -version
# Deve ser: openjdk version "17.x.x" ...
```

---

### ❌ Timeout no download dos JARs

Na primeira execução, o Spark baixa os JARs do Maven Central (~100MB). Se ocorrer timeout:
- Verifique a conexão com a internet
- Aguarde alguns minutos e tente novamente
- Os JARs ficam em cache em `~/.ivy2/` após o primeiro download

---

### ❌ `SparkSession already exists` / `Cannot call methods on a stopped SparkContext`

Execute a célula inicial do notebook (que contém `try: spark.stop()`) para encerrar sessões anteriores,
depois execute a célula de criação da SparkSession novamente.

---

### ❌ `Address already in use: port 4040`

A Spark UI tenta usar a porta 4040. Se estiver ocupada, usa 4041, 4042, etc.
Não é um erro — o Spark loga automaticamente a porta que está usando.

---

### ❌ Arquivos `metastore_db/` e `derby.log` aparecem no projeto

São criados automaticamente pelo Derby (banco de metadados embutido do Spark). Já estão
no `.gitignore`. Podem ser deletados com segurança entre execuções se necessário.

---

### ❌ Kernel do Jupyter não encontrado no VS Code

Execute novamente o registro do kernel:
```bash
uv run python -m ipykernel install --user --name spark-delta-iceberg --display-name "Python (Spark Delta Iceberg)"
```
Depois reinicie o VS Code e tente selecionar o kernel novamente.

---

## 🔗 Referências

| Recurso | Link |
|---------|------|
| Apache Spark Docs | https://spark.apache.org/docs/latest/ |
| Delta Lake Docs | https://docs.delta.io/latest/ |
| Apache Iceberg Docs | https://iceberg.apache.org/docs/latest/ |
| Repositório spark-delta (Prof.) | https://github.com/jlsilva01/spark-delta |
| Repositório spark-iceberg (Prof.) | https://github.com/jlsilva01/spark-iceberg |
| UV Documentation | https://docs.astral.sh/uv/ |
| MkDocs Material | https://squidfunk.github.io/mkdocs-material/ |
| Adoptium Java 17 | https://adoptium.net/temurin/releases/?version=17 |
