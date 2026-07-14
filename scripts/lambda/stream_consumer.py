import base64
import json
import os
from datetime import datetime, timezone

import boto3

# Essa Lambda consome os eventos do Kinesis e salva em JSONL na Bronze.
# Deixei em JSONL porque é simples de ler depois no Glue/Spark.
BUCKET = os.getenv("DATALAKE_BUCKET")
BRONZE_STREAM_PREFIX = os.getenv(
    "BRONZE_STREAM_PREFIX",
    "bronze/inep/alfabetizacao/stream_indicadores",
)

s3 = boto3.client("s3")


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    linhas_json = []

    for record in event.get("Records", []):
        data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        payload = json.loads(data)
        payload["_ingestion_ts"] = now.isoformat()
        linhas_json.append(json.dumps(payload, ensure_ascii=False))

    if not linhas_json:
        return {"statusCode": 200, "records": 0}

    key = (
        f"{BRONZE_STREAM_PREFIX}/ano={now.year}/mes={now.month:02d}/dia={now.day:02d}/"
        f"eventos_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=("\n".join(linhas_json) + "\n").encode("utf-8"),
    )

    return {"statusCode": 200, "records": len(linhas_json), "s3_key": key}
