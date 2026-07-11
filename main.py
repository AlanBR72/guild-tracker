import time
from datetime import datetime

from config import (
    ARQUIVO_ESTADO,
    BRASIL,
    HORA_ATUALIZACAO_DIARIA,
    INTERVALO_GUILDA,
    INTERVALO_HUNTED,
    INTERVALO_LOOP,
    MINUTO_ATUALIZACAO_DIARIA,
)
from guild import atualizar_visao_geral, monitorar_guilda
from storage import garantir_data_folder, carregar_json, salvar_json
from tracker import atualizar_spy_info, atualizar_spy_rank, monitorar_guildas_hunted


def executar_monitoramento_guilda():
    try:
        print("[main] monitoramento da Virtue iniciado...")
        monitorar_guilda()
        atualizar_visao_geral()
        print("[main] monitoramento da Virtue finalizado.")
    except Exception as e:
        print(f"[main] erro no monitoramento da Virtue: {e}")


def executar_monitoramento_hunted():
    try:
        print("[main] monitoramento das guildas hunted iniciado...")
        monitorar_guildas_hunted()
        print("[main] monitoramento das guildas hunted finalizado.")
    except Exception as e:
        print(f"[main] erro no monitoramento das guildas hunted: {e}")


def executar_paineis_diarios():
    try:
        print("[main] atualização diária iniciada...")
        atualizar_spy_rank()
        atualizar_spy_info()
        print("[main] atualização diária finalizada.")
    except Exception as e:
        print(f"[main] erro na atualização diária: {e}")


def executar_primeira_mensagem_trackers():
    estado = carregar_json(ARQUIVO_ESTADO, {})

    if not estado.get("primeira_msg_spy_rank"):
        print("[main] criando primeira mensagem do #spy-rank...")
        atualizar_spy_rank()
        estado["primeira_msg_spy_rank"] = True

    if not estado.get("primeira_msg_spy_info"):
        print("[main] criando primeira mensagem do #spy-info...")
        atualizar_spy_info()
        estado["primeira_msg_spy_info"] = True

    salvar_json(ARQUIVO_ESTADO, estado)


def deve_atualizar_diario(ultimo_dia):
    agora = datetime.now(BRASIL)
    janela = (
        agora.hour == HORA_ATUALIZACAO_DIARIA
        and MINUTO_ATUALIZACAO_DIARIA <= agora.minute < MINUTO_ATUALIZACAO_DIARIA + 10
    )
    return janela and ultimo_dia != agora.date(), agora.date()


def segundos_desde(ultimo, agora):
    if ultimo is None:
        return None
    return (agora - ultimo).total_seconds()


def main():
    garantir_data_folder()

    print("===================================")
    print(" Rucoy Guild Tracker iniciado")
    print(f" Virtue: a cada {INTERVALO_GUILDA // 60} minutos")
    print(f" Guildas hunted: a cada {INTERVALO_HUNTED // 60} minutos")
    print(" Painéis diários: 03:00 Brasil")
    print("===================================")

    ultimo_dia_atualizado = None
    ultima_guilda = None
    ultimo_hunted = None

    executar_primeira_mensagem_trackers()

    while True:
        agora = datetime.now(BRASIL)

        if ultima_guilda is None or segundos_desde(ultima_guilda, agora) >= INTERVALO_GUILDA:
            executar_monitoramento_guilda()
            ultima_guilda = datetime.now(BRASIL)

        if ultimo_hunted is None or segundos_desde(ultimo_hunted, agora) >= INTERVALO_HUNTED:
            executar_monitoramento_hunted()
            ultimo_hunted = datetime.now(BRASIL)

        atualizar, data_atual = deve_atualizar_diario(ultimo_dia_atualizado)
        if atualizar:
            executar_paineis_diarios()
            ultimo_dia_atualizado = data_atual

        time.sleep(INTERVALO_LOOP)


if __name__ == "__main__":
    main()
