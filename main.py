import time
from datetime import datetime

from config import (
    BRASIL,
    HORA_ATUALIZACAO_DIARIA,
    INTERVALO_MONITOR,
    MINUTO_ATUALIZACAO_DIARIA,
)
from guild import atualizar_visao_geral, monitorar_guilda
from storage import garantir_data_folder
from tracker import atualizar_peace_killers, atualizar_spy_rank


def executar_monitoramento():
    try:
        print("[main] monitoramento rápido iniciado...")
        monitorar_guilda()
    except Exception as e:
        print(f"[main] erro no monitoramento: {e}")


def executar_paineis_diarios():
    try:
        print("[main] atualização diária iniciada...")
        atualizar_visao_geral()
        atualizar_spy_rank()
        atualizar_peace_killers()
        print("[main] atualização diária finalizada.")
    except Exception as e:
        print(f"[main] erro na atualização diária: {e}")


def deve_atualizar_diario(ultimo_dia):
    agora = datetime.now(BRASIL)
    janela = (
        agora.hour == HORA_ATUALIZACAO_DIARIA
        and agora.minute >= MINUTO_ATUALIZACAO_DIARIA
        and agora.minute < MINUTO_ATUALIZACAO_DIARIA + 10
    )
    return janela and ultimo_dia != agora.date(), agora.date()


def main():
    garantir_data_folder()

    print("===================================")
    print(" Rucoy Guild Tracker iniciado")
    print(" Monitoramento: a cada 10 minutos")
    print(" Painéis diários: 03:00 Brasil")
    print("===================================")

    ultimo_dia_atualizado = None

    while True:
        executar_monitoramento()

        atualizar, data_atual = deve_atualizar_diario(ultimo_dia_atualizado)
        if atualizar:
            executar_paineis_diarios()
            ultimo_dia_atualizado = data_atual

        print(f"[main] aguardando {INTERVALO_MONITOR} segundos...")
        time.sleep(INTERVALO_MONITOR)


if __name__ == "__main__":
    main()
