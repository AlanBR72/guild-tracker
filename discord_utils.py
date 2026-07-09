import requests

from config import (
    DISCORD_LIMITE,
    WEBHOOK_ENTRADA_SAIDA,
    WEBHOOK_PEACE_KILLERS,
    WEBHOOK_MOB_XP_PEACE,
    WEBHOOK_SPY_RANK,
    WEBHOOK_UP_LEVELS,
    WEBHOOK_VISAO_GERAL,
)
from storage import carregar_json, salvar_json
from config import ARQUIVO_ESTADO

WEBHOOKS = {
    "spy_rank": WEBHOOK_SPY_RANK,
    "visao_geral": WEBHOOK_VISAO_GERAL,
    "entrada_saida": WEBHOOK_ENTRADA_SAIDA,
    "up_levels": WEBHOOK_UP_LEVELS,
    "peace_killers": WEBHOOK_PEACE_KILLERS,
    "mob_xp_peace": WEBHOOK_MOB_XP_PEACE,
}


def enviar(canal: str, mensagem: str):
    webhook = WEBHOOKS[canal]
    try:
        r = requests.post(webhook + "?wait=true", json={"content": mensagem}, timeout=20)
        if r.status_code in (200, 201):
            print(f"[discord] mensagem enviada em {canal}")
            return r.json().get("id")
        print(f"[discord] erro ao enviar em {canal}: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[discord] erro ao enviar em {canal}: {e}")
    return None


def editar(canal: str, msg_id: str, mensagem: str):
    if not msg_id:
        return enviar(canal, mensagem)

    webhook = WEBHOOKS[canal]
    try:
        r = requests.patch(f"{webhook}/messages/{msg_id}", json={"content": mensagem}, timeout=20)
        if r.status_code in (200, 204):
            print(f"[discord] mensagem atualizada em {canal}")
            return msg_id
        print(f"[discord] erro ao editar em {canal}: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[discord] erro ao editar em {canal}: {e}")
    return msg_id


def dividir_mensagem(mensagem: str, limite: int = DISCORD_LIMITE):
    partes = []
    while len(mensagem) > limite:
        corte = mensagem[:limite]
        ultimo_break = corte.rfind("\n")
        if ultimo_break != -1:
            partes.append(mensagem[:ultimo_break])
            mensagem = mensagem[ultimo_break:].lstrip("\n")
        else:
            partes.append(corte)
            mensagem = mensagem[limite:]
    if mensagem.strip():
        partes.append(mensagem)
    return partes


def enviar_em_partes(canal: str, mensagem: str):
    for parte in dividir_mensagem(mensagem):
        enviar(canal, parte)


def atualizar_painel(canal: str, estado_key: str, mensagem: str):
    estado = carregar_json(ARQUIVO_ESTADO, {})
    msg_id = estado.get(estado_key)
    novo_id = editar(canal, msg_id, mensagem)
    if novo_id:
        estado[estado_key] = novo_id
        salvar_json(ARQUIVO_ESTADO, estado)
    return novo_id
