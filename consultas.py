"""
Consultas na coleção Medicoes.

As funções deste módulo são reaproveitadas pelo menu.
Também dá para usar direto pela linha de comando:

    python consultas.py sensores
    python consultas.py sensor S001 --inicio 2026-10-20T10:00Z --fim 2026-10-20T11:00Z
    python consultas.py sensor S001 --inicio 2026-10-20T10:00Z --fim 2026-10-20T11:00Z --validas
    python consultas.py ultima
    python consultas.py falhas --inicio 2026-10-20T00:00Z --fim 2026-10-21T00:00Z
    python consultas.py plano S001 --inicio 2026-10-20T10:00Z --fim 2026-10-20T11:00Z
"""

import argparse
from datetime import datetime, timezone

from pymongo import ASCENDING, DESCENDING
from tabulate import tabulate

from conexao import get_db

CAMPOS = ["pm25", "pm10", "co2", "temperatura", "umidade"]

# Critério de medição válida: poluentes numéricos e não negativos.
# Barra tanto valores ausentes (None) quanto códigos de erro (-999).
# Será reaproveitado nas agregações da Fase 4.
FILTRO_VALIDO = {c: {"$type": "number", "$gte": 0} for c in ("pm25", "pm10", "co2")}

def parse_data(texto: str) -> datetime:
    """Converte '2026-10-20T10:00Z' ou '2026-10-20 10:00' em datetime UTC.
    Datas sem fuso são tratadas como UTC."""
    dt = datetime.fromisoformat(texto.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def fmt_data(dt: datetime) -> str:
    # O PyMongo devolve datas sem fuso (naive), mas elas estão em UTC.
    return dt.strftime("%Y-%m-%d %H:%M") + "Z"

def validar_intervalo(inicio: datetime, fim: datetime):
    if inicio >= fim:
        raise ValueError("A data inicial precisa ser anterior à data final.")

def filtro_intervalo(inicio, fim):
    # $gte no início e $lt no fim: a medição das 11:00 pertence só
    # à janela 11:00–12:00, sem contagem dupla entre janelas vizinhas.
    return {"$gte": inicio, "$lt": fim}

# consultas

def listar_sensores(db):
    return list(db.Sensores.find({}, {"modelo": 0, "grandezas": 0}).sort("_id", ASCENDING))

def sensor_existe(db, sensor_id) -> bool:
    return db.Sensores.count_documents({"_id": sensor_id}, limit=1) > 0

def medicoes_por_sensor(db, sensor_id, inicio, fim, somente_validas=False):
    """Medições de um sensor no intervalo [inicio, fim), em ordem cronológica."""
    validar_intervalo(inicio, fim)
    filtro = {"sensor_id": sensor_id, "timestamp": filtro_intervalo(inicio, fim)}
    if somente_validas:
        filtro.update(FILTRO_VALIDO)
    return list(db.Medicoes.find(filtro, {"_id": 0}).sort("timestamp", ASCENDING))

def ultima_medicao_por_sensor(db):
    """Medição mais recente de cada sensor (usa o índice sensor_id + timestamp)."""
    resultado = []
    for s in listar_sensores(db):
        doc = db.Medicoes.find_one(
            {"sensor_id": s["_id"]}, {"_id": 0}, sort=[("timestamp", DESCENDING)]
        )
        if doc:
            resultado.append(doc)
    return resultado

def medicoes_com_falha(db, inicio, fim):
    """Medições inválidas no período: o oposto do FILTRO_VALIDO."""
    validar_intervalo(inicio, fim)
    invalida = [{c: {"$not": {"$type": "number"}}} for c in ("pm25", "pm10", "co2")]
    invalida += [{c: {"$lt": 0}} for c in ("pm25", "pm10", "co2")]
    filtro = {"timestamp": filtro_intervalo(inicio, fim), "$or": invalida}
    return list(db.Medicoes.find(filtro, {"_id": 0}).sort([("sensor_id", 1), ("timestamp", 1)]))

def plano_execucao(db, sensor_id, inicio, fim):
    """Mostra se a consulta por sensor e intervalo usa o índice (IXSCAN)."""
    explain = db.command(
        "explain",
        {"find": "Medicoes",
         "filter": {"sensor_id": sensor_id, "timestamp": filtro_intervalo(inicio, fim)},
         "sort": {"timestamp": 1}},
        verbosity="executionStats",
    )
    estagios, indices = [], []

    def percorrer(no):  # o formato do plano varia entre versões do MongoDB
        if isinstance(no, dict):
            if "stage" in no:
                estagios.append(no["stage"])
            if "indexName" in no:
                indices.append(no["indexName"])
            for v in no.values():
                percorrer(v)
        elif isinstance(no, list):
            for v in no:
                percorrer(v)

    percorrer(explain["queryPlanner"]["winningPlan"])
    stats = explain["executionStats"]
    return {
        "estagios": " -> ".join(reversed(estagios)),
        "indice": ", ".join(sorted(set(indices))) or "(nenhum)",
        "documentos_examinados": stats["totalDocsExamined"],
        "documentos_retornados": stats["nReturned"],
        "tempo_ms": stats["executionTimeMillis"],
    }

# exibição

def linhas_medicoes(docs, com_sensor=False):
    linhas = []
    for d in docs:
        linha = [fmt_data(d["timestamp"])] + [d.get(c) for c in CAMPOS]
        linhas.append(([d["sensor_id"]] if com_sensor else []) + linha)
    return linhas

def imprimir_medicoes(docs, com_sensor=False):
    cab = (["Sensor"] if com_sensor else []) + ["Timestamp (UTC)", "PM2.5", "PM10",
                                                 "CO2", "Temp °C", "Umid %"]
    print(tabulate(linhas_medicoes(docs, com_sensor), headers=cab,
                   tablefmt="simple", missingval="—"))
    print(f"\n{len(docs)} medição(ões).")

def main():
    p = argparse.ArgumentParser(description="Consultas na coleção Medicoes.")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("sensores", help="Lista os sensores cadastrados")
    sub.add_parser("ultima", help="Última medição de cada sensor")

    ps = sub.add_parser("sensor", help="Medições de um sensor num intervalo")
    ps.add_argument("sensor_id")
    ps.add_argument("--inicio", required=True)
    ps.add_argument("--fim", required=True)
    ps.add_argument("--validas", action="store_true", help="Só medições válidas")

    pf = sub.add_parser("falhas", help="Medições inválidas num intervalo")
    pf.add_argument("--inicio", required=True)
    pf.add_argument("--fim", required=True)

    pp = sub.add_parser("plano", help="Plano de execução da consulta por sensor")
    pp.add_argument("sensor_id")
    pp.add_argument("--inicio", required=True)
    pp.add_argument("--fim", required=True)

    args = p.parse_args()
    db = get_db()

    try:
        if args.cmd == "sensores":
            linhas = [[s["_id"], s["localizacao"]["cidade"], s["localizacao"]["uf"],
                       s.get("descricao", ""), "sim" if s.get("ativo") else "não"]
                      for s in listar_sensores(db)]
            print(tabulate(linhas, headers=["ID", "Cidade", "UF", "Descrição", "Ativo"]))

        elif args.cmd == "ultima":
            imprimir_medicoes(ultima_medicao_por_sensor(db), com_sensor=True)

        elif args.cmd == "sensor":
            if not sensor_existe(db, args.sensor_id):
                print(f"Sensor {args.sensor_id} não encontrado.")
                return
            docs = medicoes_por_sensor(db, args.sensor_id, parse_data(args.inicio),
                                       parse_data(args.fim), args.validas)
            print(f"Sensor {args.sensor_id} — {args.inicio} até {args.fim} (fim exclusivo)\n")
            imprimir_medicoes(docs)

        elif args.cmd == "falhas":
            imprimir_medicoes(medicoes_com_falha(db, parse_data(args.inicio),
                                                 parse_data(args.fim)), com_sensor=True)

        elif args.cmd == "plano":
            plano = plano_execucao(db, args.sensor_id, parse_data(args.inicio),
                                   parse_data(args.fim))
            print(tabulate(plano.items(), headers=["Item", "Valor"]))

    except ValueError as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()