"""
Gerador de medições simuladas para a coleção Medicoes.

Gera uma medição a cada 5 minutos para cada sensor ativo cadastrado em Sensores, durante o período configurado 
(padrão: 24 h a partir de 2026-10-20T00:00:00Z). Com 6 sensores: 6 x 288 = 1.728 documentos.

Uso:
    python 02_gerar_medicoes.py
    python 02_gerar_medicoes.py --inicio 2026-10-20T00:00:00Z --horas 24 --seed 42
    python 02_gerar_medicoes.py --taxa-falhas 0     # sem medições inválidas
"""

import argparse
import math
import random
from collections import Counter
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, DESCENDING

from conexao import get_db

INTERVALO = timedelta(minutes=5)
FUSO_LOCAL = -3  # horário de Brasília (UTC-3), usado só para os padrões diários

# Perfis de poluição: valores-base que o simulador usa para cada tipo de local.
PERFIS = {
    "centro":     {"pm25": 20, "co2": 480, "temp": 0.0},
    "industrial": {"pm25": 32, "co2": 560, "temp": 0.8},
    "residencial": {"pm25": 14, "co2": 440, "temp": -0.3},
    "parque":     {"pm25": 8,  "co2": 415, "temp": -1.2},
}

# Perfil de cada sensor. Se o documento do sensor tiver o campo "perfil",
# ele tem prioridade sobre este dicionário.
PERFIL_POR_SENSOR = {
    "S001": "centro",
    "S002": "residencial",
    "S003": "industrial",
    "S004": "parque",
    "S005": "centro",
    "S006": "industrial",
}

def parse_args():
    p = argparse.ArgumentParser(description="Gera medições simuladas no MongoDB.")
    p.add_argument("--inicio", default="2026-10-20T00:00:00Z", help="Início do período em UTC (ISO 8601).")
    p.add_argument("--horas", type=int, default=24, help="Duração em horas.")
    p.add_argument("--seed", type=int, default=42, help="Semente aleatória.")
    p.add_argument("--taxa-falhas", type=float, default=0.015, help="Proporção de medições com falha simulada (0 a 1).")
    return p.parse_args()


def fator_transito(hora_local: float) -> float:
    """Entre 0 e ~1: picos de trânsito por volta das 7h30 e das 18h."""
    pico_manha = math.exp(-((hora_local - 7.5) ** 2) / 2.0)
    pico_tarde = math.exp(-((hora_local - 18.0) ** 2) / 3.0)
    return max(pico_manha, pico_tarde)


def gerar_medicao(sensor_id, perfil, ts, rnd, taxa_falhas):
    hora_local = (ts.hour + ts.minute / 60 + FUSO_LOCAL) % 24
    transito = fator_transito(hora_local)

    # Temperatura: senoide com mínimo ~3h e máximo ~15h (horário local).
    temperatura = 20 + perfil["temp"] + 6 * math.sin(2 * math.pi * (hora_local - 9) / 24)
    temperatura += rnd.gauss(0, 0.3)

    # Umidade: varia de forma inversa à temperatura.
    umidade = 75 - 2.2 * (temperatura - 20) + rnd.gauss(0, 1.5)
    umidade = min(max(umidade, 30), 98)

    # Material particulado: base do perfil, aumentada nos horários de pico.
    pm25 = perfil["pm25"] * (0.7 + 0.8 * transito) + rnd.gauss(0, 2.0)
    pm25 = max(pm25, 1.0)
    pm10 = pm25 * rnd.uniform(1.6, 2.2)  # PM10 inclui PM2.5, então PM10 >= PM2.5

    co2 = perfil["co2"] + 120 * transito + rnd.gauss(0, 12)

    doc = {
        "sensor_id": sensor_id,
        "timestamp": ts,
        "pm25": round(pm25, 1),
        "pm10": round(pm10, 1),
        "co2": round(co2),
        "temperatura": round(temperatura, 1),
        "umidade": round(umidade),
    }

    # Falha simulada: valor ausente (None) ou código de erro (-999).
    falha = None
    if rnd.random() < taxa_falhas:
        campo = rnd.choice(["pm25", "pm10", "co2"])
        doc[campo] = rnd.choice([None, -999])
        falha = campo
    return doc, falha


def main():
    args = parse_args()
    inicio = datetime.fromisoformat(args.inicio.replace("Z", "+00:00")).astimezone(timezone.utc)
    fim = inicio + timedelta(hours=args.horas)

    db = get_db()
    sensores = list(db.Sensores.find({"ativo": True}).sort("_id", ASCENDING))
    if not sensores:
        print("Nenhum sensor ativo encontrado. Rode antes o 01_seed_sensores.py.")
        return

    ids = [s["_id"] for s in sensores]

    # Idempotência: remove medições anteriores desses sensores no mesmo período,
    # para que rodar o script duas vezes não duplique os dados.
    removidos = db.Medicoes.delete_many(
        {"sensor_id": {"$in": ids}, "timestamp": {"$gte": inicio, "$lt": fim}}
    ).deleted_count

    documentos, falhas = [], Counter()
    for i, sensor in enumerate(sensores):
        sid = sensor["_id"]
        nome_perfil = sensor.get("perfil") or PERFIL_POR_SENSOR.get(sid, "centro")
        perfil = PERFIS[nome_perfil]
        rnd = random.Random(args.seed + i)  # uma semente por sensor: dados reproduzíveis

        ts = inicio
        while ts < fim:
            doc, falha = gerar_medicao(sid, perfil, ts, rnd, args.taxa_falhas)
            documentos.append(doc)
            if falha:
                falhas[sid] += 1
            ts += INTERVALO

    resultado = db.Medicoes.insert_many(documentos, ordered=False)
    db.Medicoes.create_index([("sensor_id", ASCENDING), ("timestamp", DESCENDING)])

    # Conferência
    print(f"Período: {inicio:%Y-%m-%dT%H:%M:%SZ} até {fim:%Y-%m-%dT%H:%M:%SZ}")
    print(f"Medições anteriores removidas: {removidos}")
    print(f"Documentos inseridos: {len(resultado.inserted_ids)}\n")
    print(f"{'Sensor':<8}{'Perfil':<13}{'Medições':>9}{'Com falha':>11}")
    for sensor in sensores:
        sid = sensor["_id"]
        n = db.Medicoes.count_documents(
            {"sensor_id": sid, "timestamp": {"$gte": inicio, "$lt": fim}}
        )
        nome_perfil = sensor.get("perfil") or PERFIL_POR_SENSOR.get(sid, "centro")
        print(f"{sid:<8}{nome_perfil:<13}{n:>9}{falhas[sid]:>11}")
    print(f"\nTotal na coleção Medicoes: {db.Medicoes.count_documents({})}")


if __name__ == "__main__":
    main()