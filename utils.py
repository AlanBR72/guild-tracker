from datetime import datetime, timedelta

from config import BRASIL


def agora_brasil():
    return datetime.now(BRASIL)


def data_hora_brasil():
    agora = agora_brasil()
    return agora.strftime("%d/%m/%y"), agora.strftime("%H:%M")


def data_hora_segundos_brasil():
    agora = agora_brasil()
    return agora.strftime("%d/%m/%y"), agora.strftime("%H:%M"), agora.strftime("%H:%M:%S")


def formatar_k(valor: int) -> str:
    if valor >= 1000:
        return f"{valor / 1000:.1f}k"
    return str(valor)


def dias_para_tempo(dias: int) -> str:
    anos = dias // 365
    resto_ano = dias % 365
    meses = resto_ano // 30
    resto_dias = resto_ano % 30
    partes = []

    if anos == 1:
        partes.append("1 ano")
    elif anos > 1:
        partes.append(f"{anos} anos")

    if meses == 1:
        partes.append("1 mês")
    elif meses > 1:
        partes.append(f"{meses} meses")

    if anos == 0 and resto_dias > 0:
        partes.append("1 dia" if resto_dias == 1 else f"{resto_dias} dias")

    return " e ".join(partes) if partes else "0 dias"


def detectar_classe(skill: dict):
    melee = skill.get("melee", 0)
    dist = skill.get("distance", 0)
    magic = skill.get("magic", 0)

    if melee >= dist and melee >= magic:
        return "⚔️ Melee", melee
    if dist >= melee and dist >= magic:
        return "🏹 Dist", dist
    return "🪄 Magic", magic


def segundos_ate_proximo_horario(hora: int, minuto: int = 0) -> float:
    agora = agora_brasil()
    alvo = agora.replace(hour=hora, minute=minuto, second=0, microsecond=0)
    if agora >= alvo:
        alvo += timedelta(days=1)
    return (alvo - agora).total_seconds()
