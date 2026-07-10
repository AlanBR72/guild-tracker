import time
from datetime import datetime

from config import (
    BRASIL,
    HORA_ATUALIZACAO_DIARIA,
    INTERVALO_GUILDA,
    INTERVALO_LOOP,
    INTERVALO_PEACE,
    MINUTO_ATUALIZACAO_DIARIA,
)
from guild import atualizar_visao_geral, monitorar_guilda
from storage import garantir_data_folder, carregar_json, salvar_json
from config import ARQUIVO_ESTADO
from tracker import (
    atualizar_peace_killers,
    atualizar_spy_rank,
    monitorar_mob_xp_peace,
    monitorar_movimentacoes_peace,
)


def executar_monitoramento_guilda():
    """Monitora a Virtue.

    Roda no intervalo definido por INTERVALO_GUILDA.
    - #entrada-e-saidas: atualiza o histórico se houver entrada/saída/troca de nick.
    - #up-levels: atualiza o histórico se houver up/down/quase level.
    - #visao-geral: edita o painel fixo.
    """
    try:
        print("[main] monitoramento da Virtue iniciado...")
        monitorar_guilda()
        atualizar_visao_geral()
        print("[main] monitoramento da Virtue finalizado.")
    except Exception as e:
        print(f"[main] erro no monitoramento da Virtue: {e}")


def executar_monitoramento_peace():
    """Monitora Mob XP e movimentações da Peace Killers.

    Roda no intervalo definido por INTERVALO_PEACE.
    """
    try:
        print("[main] monitoramento da Peace iniciado...")
        monitorar_mob_xp_peace()
        monitorar_movimentacoes_peace()
        print("[main] monitoramento da Peace finalizado.")
    except Exception as e:
        print(f"[main] erro no monitoramento da Peace: {e}")


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


def segundos_desde(ultimo, agora):
    if ultimo is None:
        return None
    return (agora - ultimo).total_seconds()


def main():
    garantir_data_folder()

    print("===================================")
    print(" Rucoy Guild Tracker iniciado")
    print(f" Virtue: a cada {INTERVALO_GUILDA // 60} minutos")
    print(f" Peace (Mob XP + membros): a cada {INTERVALO_PEACE // 60} minutos")
    print(" Painéis diários: 03:00 Brasil")
    print("===================================")

    ultimo_dia_atualizado = None
    ultima_guilda = None
    ultima_peace = None

    executar_primeira_mensagem_trackers()

    while True:
        agora = datetime.now(BRASIL)

        # Virtue em intervalo próprio.
        if ultima_guilda is None or segundos_desde(ultima_guilda, agora) >= INTERVALO_GUILDA:
            executar_monitoramento_guilda()
            ultima_guilda = datetime.now(BRASIL)

        # Peace/Mob XP em intervalo próprio.
        if ultima_peace is None or segundos_desde(ultima_peace, agora) >= INTERVALO_PEACE:
            executar_monitoramento_peace()
            ultima_peace = datetime.now(BRASIL)

        atualizar, data_atual = deve_atualizar_diario(ultimo_dia_atualizado)
        if atualizar:
            executar_paineis_diarios()
            ultimo_dia_atualizado = data_atual

        time.sleep(INTERVALO_LOOP)


if __name__ == "__main__":
    main()
