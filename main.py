import time
from datetime import datetime
import pytz

from guild import (
    monitorar_guilda,
    atualizar_visao_geral
)

from tracker import (
    atualizar_spy_rank,
    atualizar_peace_killers
)

BRASIL = pytz.timezone("America/Sao_Paulo")

HORA_DAILY = 3
INTERVALO_MONITOR = 600  # 10 minutos


def executar_monitoramento():
    """
    Executa a auditoria rápida da guilda.
    Envia mensagens somente se houver alterações.
    """
    try:
        monitorar_guilda()

    except Exception as e:
        print(f"[Monitoramento] Erro: {e}")


def executar_paineis():
    """
    Atualiza os painéis diários.
    """
    try:
        atualizar_visao_geral()
        atualizar_spy_rank()
        atualizar_peace_killers()

    except Exception as e:
        print(f"[Painéis] Erro: {e}")


def main():

    print("===================================")
    print(" Rucoy Guild Tracker iniciado")
    print("===================================")

    ultimo_dia = None

    while True:

        agora = datetime.now(BRASIL)

        # Monitoramento contínuo
        executar_monitoramento()

        # Atualização diária às 03:00
        if (
            agora.hour == HORA_DAILY
            and agora.minute < 10
            and ultimo_dia != agora.date()
        ):

            executar_paineis()
            ultimo_dia = agora.date()

        time.sleep(INTERVALO_MONITOR)


if __name__ == "__main__":
    main()
