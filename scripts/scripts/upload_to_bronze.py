import os

import boto3
from config import BRONZE_PREFIX, BUCKET, LOCAL_FILES


def upload_file(s3, local_path, s3_key):
    # Print simples mesmo, porque para o trabalho ajuda a ver o que está subindo.
    print(f"Enviando: {local_path}")
    print(f"Destino : s3://{BUCKET}/{s3_key}")
    s3.upload_file(local_path, BUCKET, s3_key)


def main():
    # INPUT_DIR é a pasta onde deixei os CSVs baixados.
    # Se não informar, ele procura na pasta atual.
    input_dir = os.getenv("INPUT_DIR", ".")
    s3 = boto3.client("s3")

    for entity, filename in LOCAL_FILES.items():
        local_path = os.path.join(input_dir, filename)

        if not os.path.exists(local_path):
            raise FileNotFoundError(
                f"Não achei o arquivo {filename}. Verificar INPUT_DIR: {input_dir}"
            )

        # Mantive dt_carga=manual só para deixar claro que essa carga foi feita localmente.
        s3_key = f"{BRONZE_PREFIX}/{entity}/dt_carga=manual/{filename}"
        upload_file(s3, local_path, s3_key)

    print("Upload para a Bronze finalizado.")


if __name__ == "__main__":
    main()
