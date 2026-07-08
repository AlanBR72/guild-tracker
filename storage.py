import json
import os
from typing import Any

from config import DATA_FOLDER


def garantir_data_folder() -> None:
    os.makedirs(DATA_FOLDER, exist_ok=True)


def carregar_json(caminho: str, padrao: Any):
    garantir_data_folder()
    if not os.path.exists(caminho):
        return padrao
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[storage] erro ao carregar {caminho}: {e}")
        return padrao


def salvar_json(caminho: str, data: Any) -> None:
    garantir_data_folder()
    tmp = caminho + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, caminho)
