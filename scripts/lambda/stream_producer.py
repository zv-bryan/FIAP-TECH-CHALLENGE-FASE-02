import json
import os
import random
import time
from datetime import datetime, timezone

import boto3

# Essa Lambda simula a chegada de eventos em tempo quase real.
# Não é uma fonte oficial, é só para representar o cenário de streaming pedido no desafio.
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "tc-alfabetizacao-indicadores")


def montar_evento():
    # Usei alguns municípios grandes só para gerar exemplos fáceis de consultar depois.
    ano = random.choice([2024, 2025])
    id_municipio = random.choice([3550308, 3304557, 3106200, 2927408, 2304400])

    return {
        "event_type": "indicador_atualizado",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "ano": ano,
        "id_municipio": id_municipio,
        "rede": random.choice(["Pública", "Municipal", "Estadual"]),
        "taxa_alfabetizacao": round(random.uniform(45, 85), 2),
        "media_portugues": round(random.uniform(700, 790), 4),
        "fonte": "simulacao_streaming",
    }


def lambda_handler(event, context):
    kinesis = boto3.client("kinesis")
    qtd = int(os.getenv("EVENTS_PER_RUN", "10"))

    for _ in range(qtd):
        payload = montar_evento()

        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            PartitionKey=str(payload["id_municipio"]),
        )

        # Sleep pequeno só para os eventos não ficarem exatamente no mesmo instante.
        time.sleep(0.1)

    return {"statusCode": 200, "events_sent": qtd}
