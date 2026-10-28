from datetime import datetime, timezone
from conexao import get_db
import redis

db = get_db()
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

inicio_periodo_str = "2026-10-20T10:00:00Z"
fim_periodo_str = "2026-10-20T11:00:00Z"

inicio_dt = datetime.fromisoformat(inicio_periodo_str.replace("Z", "+00:00")).astimezone(timezone.utc)
fim_dt = datetime.fromisoformat(fim_periodo_str.replace("Z", "+00:00")).astimezone(timezone.utc)

print("--- A PROCESSAR PIPELINES DE AGREGAÇÃO ---")

# Pipeline por Sensor
pipeline_sensor = [
    {
        "$match": {
            "timestamp": {"$gte": inicio_dt, "$lt": fim_dt},
            "pm25": {"$gte": 0},
            "pm10": {"$gte": 0},
            "co2": {"$gte": 0}
        }
    },
    {
        "$group": {
            "_id": "$sensor_id",
            "qt_medicoes": {"$sum": 1},
            "pm25_media": {"$avg": "$pm25"},
            "pm10_media": {"$avg": "$pm10"},
            "co2_media": {"$avg": "$co2"},
            "temperatura_media": {"$avg": "$temperatura"},
            "umidade_media": {"$avg": "$umidade"},
            "pm25_max": {"$max": "$pm25"},
            "pm10_max": {"$max": "$pm10"}
        }
    }
]

resultados_sensores = list(db.Medicoes.aggregate(pipeline_sensor))

for dados in resultados_sensores:
    sensor_id = dados["_id"]
    chave_redis = f"qualidadear:sensor:{sensor_id}"
    
    redis_client.hset(
        chave_redis,
        mapping={
            "inicio_periodo": inicio_periodo_str,
            "fim_periodo": fim_periodo_str,
            "qt_medicoes": dados["qt_medicoes"],
            "pm25_media": round(dados["pm25_media"], 2),
            "pm10_media": round(dados["pm10_media"], 2),
            "co2_media": round(dados["co2_media"], 2),
            "temperatura_media": round(dados["temperatura_media"], 2),
            "umidade_media": round(dados["umidade_media"], 2),
            "pm25_max": dados["pm25_max"],
            "pm10_max": dados["pm10_max"]
        }
    )

# Pipeline por Cidade
pipeline_cidade = [
    {
        "$match": {
            "timestamp": {"$gte": inicio_dt, "$lt": fim_dt},
            "pm25": {"$gte": 0},
            "pm10": {"$gte": 0},
            "co2": {"$gte": 0}
        }
    },
    {
        "$lookup": {
            "from": "Sensores",
            "localField": "sensor_id",
            "foreignField": "_id",
            "as": "info_sensor"
        }
    },
    {"$unwind": "$info_sensor"},
    {
        "$group": {
            "_id": {
                "cidade": "$info_sensor.localizacao.cidade",
                "uf": "$info_sensor.localizacao.uf"
            },
            "qt_medicoes": {"$sum": 1},
            "pm25_media": {"$avg": "$pm25"},
            "pm10_media": {"$avg": "$pm10"},
            "co2_media": {"$avg": "$co2"},
            "temperatura_media": {"$avg": "$temperatura"},
            "umidade_media": {"$avg": "$umidade"},
            "pm25_max": {"$max": "$pm25"},
            "pm10_max": {"$max": "$pm10"}
        }
    }
]

resultados_cidades = list(db.Medicoes.aggregate(pipeline_cidade))

for dados in resultados_cidades:
    cidade_nome = dados["_id"]["cidade"].lower()
    uf = dados["_id"]["uf"]
    
    cidade_normalizada = (
        cidade_nome.replace("ç", "c")
                   .replace("ó", "o")
                   .replace("á", "a")
                   .replace("í", "i")
                   .replace("ô", "o")
                   .replace("ê", "e")
    )
    
    chave_redis = f"qualidadear:cidade:{uf}:{cidade_normalizada}"
    
    redis_client.hset(
        chave_redis,
        mapping={
            "inicio_periodo": inicio_periodo_str,
            "fim_periodo": fim_periodo_str,
            "qt_medicoes": dados["qt_medicoes"],
            "pm25_media": round(dados["pm25_media"], 2),
            "pm10_media": round(dados["pm10_media"], 2),
            "co2_media": round(dados["co2_media"], 2),
            "temperatura_media": round(dados["temperatura_media"], 2),
            "umidade_media": round(dados["umidade_media"], 2),
            "pm25_max": dados["pm25_max"],
            "pm10_max": dados["pm10_max"]
        }
    )

# Apresentação no Console
print("\n" + "="*65)
print("     RESULTADOS DOS RESUMOS NO REDIS (EXIBIÇÃO VIA CONSOLE)")
print("="*65)

print("\n--- RESUMOS POR SENSOR ---")
for dados in resultados_sensores:
    chave = f"qualidadear:sensor:{dados['_id']}"
    resumo = redis_client.hgetall(chave)
    print(f"\nChave Redis: {chave}")
    for campo, valor in resumo.items():
        print(f"  {campo}: {valor}")

print("\n--- RESUMOS POR CIDADE ---")
for dados in resultados_cidades:
    cidade_nome = dados["_id"]["cidade"].lower()
    cidade_norm = (
        cidade_nome.replace("ç", "c")
                   .replace("ó", "o")
                   .replace("á", "a")
                   .replace("í", "i")
                   .replace("ô", "o")
                   .replace("ê", "e")
    )
    chave = f"qualidadear:cidade:{dados['_id']['uf']}:{cidade_norm}"
    resumo = redis_client.hgetall(chave)
    print(f"\nChave Redis: {chave}")
    for campo, valor in resumo.items():
        print(f"  {campo}: {valor}")
