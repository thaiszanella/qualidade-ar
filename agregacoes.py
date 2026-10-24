"""
Agregações e sumarização com o pipeline de agregação do MongoDB.

As funções deste módulo são reaproveitadas pela sincronização com o Redis e pelo menu. Uso pela linha de comando:

    python agregacoes.py sensor S001 --inicio 2026-10-20T10:00Z
    python agregacoes.py horario-sensor --sensor S001 --inicio 2026-10-20T00:00Z --fim 2026-10-21T00:00Z
    python agregacoes.py cidade --inicio 2026-10-20T10:00Z
    python agregacoes.py cidade --uf SC --cidade Chapeco --inicio 2026-10-20T10:00Z
    python agregacoes.py horario-cidade --inicio 2026-10-20T00:00Z --fim 2026-10-21T00:00Z
    python agregacoes.py comparar --inicio 2026-10-20T10:00Z
    python agregacoes.py ultima-hora

Se --fim for omitido, a janela é de 1 hora a partir de --inicio.
"""

import argparse
from datetime import timedelta

from tabulate import tabulate
from unidecode import unidecode

from conexao import get_db
from consultas import FILTRO_VALIDO, fmt_data, parse_data, validar_intervalo

# Acumuladores comuns a todos os resumos ($group).
ACUMULADORES = {
    "qt_medicoes": {"$sum": 1},
    "pm25_media": {"$avg": "$pm25"},
    "pm10_media": {"$avg": "$pm10"},
    "co2_media": {"$avg": "$co2"},
    "temperatura_media": {"$avg": "$temperatura"},
    "umidade_media": {"$avg": "$umidade"},
    "pm25_max": {"$max": "$pm25"},
    "pm10_max": {"$max": "$pm10"},
}

CAMPOS_RESUMO = list(ACUMULADORES)


def projecao_resumo(identificacao: dict, inicio, fim) -> dict:
    """Estágio $project: identificação + período + métricas arredondadas."""
    proj = {"_id": 0, **identificacao, "inicio_periodo": inicio, "fim_periodo": fim,
            "qt_medicoes": 1}
    for campo in CAMPOS_RESUMO[1:]:
        proj[campo] = {"$round": [f"${campo}", 1]}
    return {"$project": proj}


def match_janela(inicio, fim, extra=None) -> dict:
    """Estágio $match: janela [inicio, fim) + apenas medições válidas."""
    filtro = {"timestamp": {"$gte": inicio, "$lt": fim}, **FILTRO_VALIDO}
    if extra:
        filtro.update(extra)
    return {"$match": filtro}


# Junção Medicoes -> Sensores pela identificação do sensor.
LOOKUP_SENSOR = [
    {"$lookup": {"from": "Sensores", "localField": "sensor_id",
                 "foreignField": "_id", "as": "sensor"}},
    {"$unwind": "$sensor"},
]


def ids_da_cidade(db, uf, cidade):
    """IDs dos sensores de uma cidade, comparando sem acentos e sem caixa
    (assim 'Chapeco', 'chapecó' e 'Chapecó' funcionam)."""
    alvo = unidecode(cidade).strip().lower()
    return [s["_id"] for s in db.Sensores.find({"localizacao.uf": uf.upper()})
            if unidecode(s["localizacao"]["cidade"]).lower() == alvo]


# por sensor

def resumo_sensor(db, inicio, fim, sensor_id=None):
    """Resumo de cada sensor (ou de um só) numa janela fixa."""
    validar_intervalo(inicio, fim)
    extra = {"sensor_id": sensor_id} if sensor_id else None
    pipeline = [
        match_janela(inicio, fim, extra),
        {"$group": {"_id": "$sensor_id", **ACUMULADORES}},
        projecao_resumo({"sensor_id": "$_id"}, {"$literal": inicio}, {"$literal": fim}),
        {"$sort": {"sensor_id": 1}},
    ]
    return list(db.Medicoes.aggregate(pipeline))


def resumos_horarios_sensor(db, inicio, fim, sensor_id=None):
    """Um resumo por sensor e por hora, numa única agregação ($dateTrunc)."""
    validar_intervalo(inicio, fim)
    extra = {"sensor_id": sensor_id} if sensor_id else None
    hora = {"$dateTrunc": {"date": "$timestamp", "unit": "hour", "timezone": "UTC"}}
    pipeline = [
        match_janela(inicio, fim, extra),
        {"$group": {"_id": {"sensor_id": "$sensor_id", "hora": hora}, **ACUMULADORES}},
        projecao_resumo({"sensor_id": "$_id.sensor_id"}, "$_id.hora",
                        {"$dateAdd": {"startDate": "$_id.hora", "unit": "hour", "amount": 1}}),
        {"$sort": {"sensor_id": 1, "inicio_periodo": 1}},
    ]
    return list(db.Medicoes.aggregate(pipeline))


# por cidade

def resumo_cidade(db, inicio, fim, uf=None, cidade=None):
    """Resumo de cada cidade (ou de uma só) numa janela fixa.

    O $group é feito diretamente sobre TODAS as medições válidas da cidade,
    então as médias são globais (ponderadas pela quantidade de medições de
    cada sensor), e não a média simples das médias dos sensores.
    """
    validar_intervalo(inicio, fim)
    extra = None
    if uf and cidade:
        ids = ids_da_cidade(db, uf, cidade)
        if not ids:
            return []
        # Filtrar pelos IDs ANTES do $lookup aproveita o índice e reduz a junção.
        extra = {"sensor_id": {"$in": ids}}

    pipeline = [
        match_janela(inicio, fim, extra),
        *LOOKUP_SENSOR,
        {"$group": {"_id": {"uf": "$sensor.localizacao.uf",
                            "cidade": "$sensor.localizacao.cidade"},
                    "sensores": {"$addToSet": "$sensor_id"},
                    **ACUMULADORES}},
        projecao_resumo({"uf": "$_id.uf", "cidade": "$_id.cidade",
                         "qt_sensores": {"$size": "$sensores"}},
                        {"$literal": inicio}, {"$literal": fim}),
        {"$sort": {"uf": 1, "cidade": 1}},
    ]
    return list(db.Medicoes.aggregate(pipeline))


def resumos_horarios_cidade(db, inicio, fim):
    """Um resumo por cidade e por hora."""
    validar_intervalo(inicio, fim)
    hora = {"$dateTrunc": {"date": "$timestamp", "unit": "hour", "timezone": "UTC"}}
    pipeline = [
        match_janela(inicio, fim),
        *LOOKUP_SENSOR,
        {"$group": {"_id": {"uf": "$sensor.localizacao.uf",
                            "cidade": "$sensor.localizacao.cidade", "hora": hora},
                    **ACUMULADORES}},
        projecao_resumo({"uf": "$_id.uf", "cidade": "$_id.cidade"}, "$_id.hora",
                        {"$dateAdd": {"startDate": "$_id.hora", "unit": "hour", "amount": 1}}),
        {"$sort": {"uf": 1, "cidade": 1, "inicio_periodo": 1}},
    ]
    return list(db.Medicoes.aggregate(pipeline))


def comparar_medias_cidade(db, inicio, fim):
    """Demonstra a diferença entre:
      - média global (ponderada): soma de todas as medições / total de medições
      - média simples das médias de cada sensor
    Elas só coincidem quando todos os sensores têm a mesma quantidade de medições.
    """
    validar_intervalo(inicio, fim)
    poluentes = ["pm25", "pm10", "co2"]

    # 1º $group: um documento por sensor, com soma, média e quantidade.
    grupo_sensor = {"_id": "$sensor_id", "qt": {"$sum": 1}}
    for p in poluentes:
        grupo_sensor[f"soma_{p}"] = {"$sum": f"${p}"}
        grupo_sensor[f"media_{p}"] = {"$avg": f"${p}"}

    # 2º $group: agrupa os sensores por cidade.
    grupo_cidade = {"_id": {"uf": "$sensor.localizacao.uf",
                            "cidade": "$sensor.localizacao.cidade"},
                    "qt_medicoes": {"$sum": "$qt"},
                    "qts_por_sensor": {"$push": {"k": "$_id", "v": "$qt"}}}
    for p in poluentes:
        grupo_cidade[f"soma_{p}"] = {"$sum": f"$soma_{p}"}
        grupo_cidade[f"{p}_media_simples"] = {"$avg": f"$media_{p}"}

    proj = {"_id": 0, "uf": "$_id.uf", "cidade": "$_id.cidade", "qt_medicoes": 1,
            "qts_por_sensor": {"$arrayToObject": "$qts_por_sensor"}}
    for p in poluentes:
        proj[f"{p}_media_global"] = {"$round": [{"$divide": [f"$soma_{p}", "$qt_medicoes"]}, 3]}
        proj[f"{p}_media_simples"] = {"$round": [f"${p}_media_simples", 3]}

    pipeline = [
        match_janela(inicio, fim),
        {"$group": grupo_sensor},
        {"$lookup": {"from": "Sensores", "localField": "_id",
                     "foreignField": "_id", "as": "sensor"}},
        {"$unwind": "$sensor"},
        {"$group": grupo_cidade},
        {"$project": proj},
        {"$sort": {"uf": 1, "cidade": 1}},
    ]
    return list(db.Medicoes.aggregate(pipeline))


# janela recente

def ultima_hora_completa(db):
    """Janela [inicio, fim) da última hora completa com dados no MongoDB.
    Ex.: última medição às 23:55 -> janela 23:00–00:00;
         última medição às 10:20 -> janela 09:00–10:00 (10h ainda incompleta)."""
    ultima = db.Medicoes.find_one({}, {"timestamp": 1}, sort=[("timestamp", -1)])
    if not ultima:
        return None
    ts = ultima["timestamp"] + timedelta(minutes=5)  # instante seguinte à última leitura
    fim = ts.replace(minute=0, second=0, microsecond=0)
    return fim - timedelta(hours=1), fim


# exibição

CABECALHO = ["Início", "Fim", "Qt", "PM2.5 méd", "PM10 méd", "CO2 méd",
             "Temp méd", "Umid méd", "PM2.5 máx", "PM10 máx"]


def linha_resumo(r):
    return [fmt_data(r["inicio_periodo"]), fmt_data(r["fim_periodo"])] + \
           [r[c] for c in CAMPOS_RESUMO]


def imprimir_resumos_sensor(resumos):
    linhas = [[r["sensor_id"]] + linha_resumo(r) for r in resumos]
    print(tabulate(linhas, headers=["Sensor"] + CABECALHO, floatfmt=".1f"))
    print(f"\n{len(resumos)} resumo(s).")


def imprimir_resumos_cidade(resumos):
    linhas = [[f'{r["cidade"]}/{r["uf"]}'] + linha_resumo(r) for r in resumos]
    print(tabulate(linhas, headers=["Cidade"] + CABECALHO, floatfmt=".1f"))
    print(f"\n{len(resumos)} resumo(s).")


def imprimir_comparacao(comparacoes):
    for c in comparacoes:
        print(f'\n{c["cidade"]}/{c["uf"]} — {c["qt_medicoes"]} medições válidas')
        print("  Medições por sensor:", ", ".join(f"{k}={v}" for k, v in c["qts_por_sensor"].items()))
        linhas = []
        for p, nome in [("pm25", "PM2.5"), ("pm10", "PM10"), ("co2", "CO2")]:
            g, s = c[f"{p}_media_global"], c[f"{p}_media_simples"]
            linhas.append([nome, g, s, round(g - s, 3)])
        print(tabulate(linhas, headers=["Poluente", "Média global", "Média das médias",
                                        "Diferença"], floatfmt=".3f"))


def main():
    p = argparse.ArgumentParser(description="Agregações de qualidade do ar.")
    sub = p.add_subparsers(dest="cmd", required=True)

    def janela(sp):
        sp.add_argument("--inicio", required=True)
        sp.add_argument("--fim", help="Padrão: 1 hora após --inicio")

    ps = sub.add_parser("sensor", help="Resumo por sensor numa janela")
    ps.add_argument("sensor_id", nargs="?")
    janela(ps)

    phs = sub.add_parser("horario-sensor", help="Resumos horários por sensor")
    phs.add_argument("--sensor")
    janela(phs)

    pc = sub.add_parser("cidade", help="Resumo por cidade numa janela")
    pc.add_argument("--uf")
    pc.add_argument("--cidade")
    janela(pc)

    janela(sub.add_parser("horario-cidade", help="Resumos horários por cidade"))
    janela(sub.add_parser("comparar", help="Média global x média das médias"))
    sub.add_parser("ultima-hora", help="Resumos da última hora completa")

    args = p.parse_args()
    db = get_db()

    try:
        if args.cmd == "ultima-hora":
            jan = ultima_hora_completa(db)
            if not jan:
                print("Não há medições no banco.")
                return
            ini, fim = jan
            print(f"Última hora completa: {fmt_data(ini)} até {fmt_data(fim)}\n")
            imprimir_resumos_sensor(resumo_sensor(db, ini, fim))
            print()
            imprimir_resumos_cidade(resumo_cidade(db, ini, fim))
            return

        ini = parse_data(args.inicio)
        fim = parse_data(args.fim) if args.fim else ini + timedelta(hours=1)

        if args.cmd == "sensor":
            imprimir_resumos_sensor(resumo_sensor(db, ini, fim, args.sensor_id))
        elif args.cmd == "horario-sensor":
            imprimir_resumos_sensor(resumos_horarios_sensor(db, ini, fim, args.sensor))
        elif args.cmd == "cidade":
            if bool(args.uf) != bool(args.cidade):
                print("Informe --uf e --cidade juntos (ou nenhum dos dois).")
                return
            resumos = resumo_cidade(db, ini, fim, args.uf, args.cidade)
            if not resumos:
                print("Nenhum dado encontrado para essa cidade/período.")
                return
            imprimir_resumos_cidade(resumos)
        elif args.cmd == "horario-cidade":
            imprimir_resumos_cidade(resumos_horarios_cidade(db, ini, fim))
        elif args.cmd == "comparar":
            imprimir_comparacao(comparar_medias_cidade(db, ini, fim))

    except ValueError as e:
        print(f"Erro: {e}")


if __name__ == "__main__":
    main()