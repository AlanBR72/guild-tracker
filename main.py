import time
from datetime import datetime

from config import (
    BRASIL,
    HORA_ATUALIZACAO_DIARIA,
    INTERVALO_MONITOR,
    MINUTO_ATUALIZACAO_DIARIA,
)
from guild import atualizar_visao_geral, monitorar_guilda
from storage import garantir_data_folder, carregar_json, salvar_json
from config import ARQUIVO_ESTADO
from tracker import atualizar_peace_killers, atualizar_spy_rank, monitorar_mob_xp_peace


def executar_monitoramento():
    try:
        print("[main] monitoramento rápido iniciado...")
        monitorar_guilda()
        monitorar_mob_xp_peace()

        # #visao-geral: cria a mensagem na primeira execução
        # e depois edita a mesma mensagem a cada 10 minutos.
        atualizar_visao_geral()

    except Exception as e:
        print(f"[main] erro no monitoramento: {e}")


def executar_paineis_diarios():
    try:
        print("[main] atualização diária iniciada...")

        # #spy-rank e #peace-killers: criam NOVA mensagem às 03:00.
        atualizar_spy_rank()
        atualizar_peace_killers()

        print("[main] atualização diária finalizada.")
    except Exception as e:
        print(f"[main] erro na atualização diária: {e}")


def executar_primeira_mensagem_trackers():
    """Cria a primeira mensagem do #spy-rank e #peace-killers apenas uma vez.

    Depois disso, esses canais passam a receber uma mensagem nova somente às 03:00.
    """
    estado = carregar_json(ARQUIVO_ESTADO, {})

    if not estado.get("primeira_msg_spy_rank"):
        print("[main] criando primeira mensagem do #spy-rank...")
        atualizar_spy_rank()
        estado["primeira_msg_spy_rank"] = True

    if not estado.get("primeira_msg_peace_killers"):
        print("[main] criando primeira mensagem do #peace-killers...")
        atualizar_peace_killers()
        estado["primeira_msg_peace_killers"] = True

    salvar_json(ARQUIVO_ESTADO, estado)


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
    print(" Monitoramento: a cada 5 minutos")
    print(" Painéis diários: 03:00 Brasil")
    print("===================================")

    ultimo_dia_atualizado = None

    executar_primeira_mensagem_trackers()

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
