import requests

from config import (
    WEBHOOK_SPY_RANK,
    WEBHOOK_VISAO_GERAL,
    WEBHOOK_ENTRADA_SAIDA,
    WEBHOOK_UP_LEVELS,
    WEBHOOK_PEACE_KILLERS,
    DISCORD_LIMITE
)

# =========================
# ENVIAR MENSAGEM
# =========================

def enviar(webhook, mensagem):

    try:

        r = requests.post(
            webhook + "?wait=true",
            json={"content": mensagem},
            timeout=15
        )

        if r.status_code in (200, 201):

            print("Mensagem enviada.")

            return r.json()["id"]

        print(f"Erro Discord: {r.text}")

        return None

    except Exception as e:

        print(f"Erro ao enviar mensagem: {e}")

        return None


# =========================
# EDITAR MENSAGEM
# =========================

def editar(webhook, msg_id, mensagem):

    if not msg_id:
        return enviar(webhook, mensagem)

    try:

        url = f"{webhook}/messages/{msg_id}"

        r = requests.patch(
            url,
            json={"content": mensagem},
            timeout=15
        )

        if r.status_code in (200, 204):

            print("Mensagem atualizada.")

            return msg_id

        print(f"Erro ao editar: {r.text}")

        return msg_id

    except Exception as e:

        print(f"Erro ao editar mensagem: {e}")

        return msg_id


# =========================
# DIVIDIR MENSAGENS GRANDES
# =========================

def enviar_em_partes(webhook, mensagem):

    partes = []

    while len(mensagem) > DISCORD_LIMITE:

        corte = mensagem[:DISCORD_LIMITE]

        ultimo = corte.rfind("\n")

        if ultimo == -1:

            partes.append(corte)

            mensagem = mensagem[DISCORD_LIMITE:]

        else:

            partes.append(mensagem[:ultimo])

            mensagem = mensagem[ultimo:]

    partes.append(mensagem)

    for parte in partes:

        enviar(webhook, parte)


# =========================
# CANAIS
# =========================

def enviar_spy_rank(msg):

    enviar_em_partes(WEBHOOK_SPY_RANK, msg)


def atualizar_visao_geral(msg, msg_id):

    return editar(
        WEBHOOK_VISAO_GERAL,
        msg_id,
        msg
    )


def enviar_entrada_saida(msg):

    enviar(
        WEBHOOK_ENTRADA_SAIDA,
        msg
    )


def enviar_up_levels(msg):

    enviar(
        WEBHOOK_UP_LEVELS,
        msg
    )


def atualizar_peace_killers(msg, msg_id):

    return editar(
        WEBHOOK_PEACE_KILLERS,
        msg_id,
        msg
    )
