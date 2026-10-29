# Monitoramento da Qualidade do Ar

Bancos de Dados e Novas Tecnologias — Unochapecó (Prof. Monica Tissiani De Toni Pereira)

## Integrantes

- Thais Zanella
- Ana Clara Jesus

## Estrutura

Pasta: codigo
Conteúdo: Scripts Python, `testes/` e `dados/sensores.json`

Pasta: docs
Conteúdo: Relatório técnico e diagrama de arquitetura

## Como executar

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
docker compose up -d
cd codigo
python 01_seed_sensores.py
python 02_gerar_medicoes.py
python 03_processar_agregacao_redis.py
```

Consultas e agregações: `python consultas.py -h` e `python agregacoes.py -h`.
