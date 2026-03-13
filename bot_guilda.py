import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
WEBHOOK = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"

ARQUIVO_ESTADO = "estado_msg.json"
INTERVALO = 86400  # 24h
THREADS = 10

BRASIL = pytz.timezone("America/Sao_Paulo")

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})


# =========================
# ESTADO (ID da msg no Discord)
# =========================
def salvar_estado(data):
    with open(ARQUIVO_ESTADO, "w") as f:
        json.dump(data, f)


def carregar_estado():
    if not os.path.exists(ARQUIVO_ESTADO):
        return {}
    with open(ARQUIVO_ESTADO, "r") as f:
        return json.load(f)


estado = carregar_estado()
mensagem_id = estado.get("msg_id")


# =========================
# DISCORD
# =========================
def enviar(msg):
    global mensagem_id
    r = requests.post(WEBHOOK + "?wait=true", json={"content": msg})
    if r.status_code in (200, 201):
        mensagem_id = r.json()["id"]
        salvar_estado({"msg_id": mensagem_id})
        print("Mensagem criada no Discord")
    else:
        print("Erro ao enviar mensagem:", r.text)


def editar(msg):
    if not mensagem_id:
        enviar(msg)
        return
    url = WEBHOOK + "/messages/" + mensagem_id
    r = requests.patch(url, json={"content": msg})
    if r.status_code in (200, 204):
        print("Mensagem atualizada")
    else:
        print("Erro ao editar mensagem:", r.text)


# =========================
# PEGAR MEMBROS DA GUILDA
# =========================
def pegar_membros():
    r = session.get(GUILD_URL, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")

    membros = []
    guild_datas = {}

    # tabela da guilda
    linhas = soup.select("table tr")

    for row in linhas[1:]:
        cols = row.find_all("td")
        link = row.select_one("a[href*='/characters/']")

        if not link or len(cols) < 3:
            continue

        nome = link.get_text(strip=True)
        join_text = cols[2].get_text(strip=True)

        try:
            join_date = datetime.strptime(join_text, "%b %d, %Y")
            join_date = BRASIL.localize(join_date)
            guild_datas[nome] = join_date
            membros.append(nome)
        except:
            continue

    return membros, guild_datas


# =========================
# PEGAR LAST ONLINE (CORRIGIDO)
# =========================
def last_online_requests(nome):
    try:
        url = f"https://www.rucoyonline.com/characters/{nome.replace(' ','%20')}"
        r = session.get(url, timeout=15)

        soup = BeautifulSoup(r.text, "html.parser")

        linhas = soup.select("table.character-table tr")

        for row in linhas:
            cols = row.find_all("td")

            if len(cols) >= 2:
                titulo = cols[0].get_text(strip=True).lower()

                if titulo == "last online":
                    texto = cols[1].get_text(strip=True).lower()

                    if "currently online" in texto:
                        return None

                    match = re.search(r"(\d+)", texto)
                    if not match:
                        return None

                    numero = int(match.group(1))

                    if "day" in texto:
                        return numero
                    elif "week" in texto:
                        return numero * 7
                    elif "month" in texto:
                        return numero * 30
                    elif "year" in texto:
                        return numero * 365

        return None

    except Exception as e:
        print(f"Erro ao pegar {nome}: {e}")
        return None


# =========================
# ANALISAR GUILDA
# =========================
def analisar():
    membros, guild_datas = pegar_membros()
    print(f"{len(membros)} membros encontrados")

    in20 = []
    in10 = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(last_online_requests, m): m for m in membros}

        for future in as_completed(futures):
            nome = futures[future]
            dias = future.result()

            if dias is None:
                continue

            if dias >= 20:
                in20.append((nome, dias))
            elif dias >= 10:
                in10.append((nome, dias))

    # membros mais antigos
    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]

    # membros sem tag
    hoje = datetime.now(BRASIL)
    membros_sem_tag = []

    for nome, join_date in guild_datas.items():
        dias_na_guilda = (hoje - join_date).days

        if (
            dias_na_guilda > 20
            and "virtue" not in nome.lower()
            and "culpa" not in nome.lower()
        ):
            membros_sem_tag.append((nome, dias_na_guilda, join_date))

    return in20, in10, antigos, membros_sem_tag


# =========================
# GERAR MENSAGEM
# =========================
def dias_para_tempo(dias):

    anos = dias // 365
    resto_ano = dias % 365

    meses = resto_ano // 30
    resto_dias = resto_ano % 30

    partes = []

    if anos > 0:
        if anos == 1:
            partes.append("1 ano")
        else:
            partes.append(f"{anos} anos")

    if meses > 0:
        if meses == 1:
            partes.append("1 mês")
        else:
            partes.append(f"{meses} meses")

    if anos == 0 and resto_dias > 0:
        if resto_dias == 1:
            partes.append("1 dia")
        else:
            partes.append(f"{resto_dias} dias")

    return " e ".join(partes)
    
def gerar_msg(in20, in10, antigos, membros_sem_tag):
    agora = datetime.now(BRASIL)
    data = agora.strftime("%d/%m/%Y")
    hora = agora.strftime("%H:%M")

    msg = f"""📊 **Auditoria da Guilda**

🕒 Atualizado em: {data} às {hora} (Brasil)

❌ **Inativos +20 dias**
"""

if in20:
    for nome, dias in sorted(in20, key=lambda x: x[1], reverse=True):

        if dias >= 30:
            dias_txt = "30+ dias"
        else:
            dias_txt = f"{dias} dias"

        msg += f"{nome} — {dias_txt}\n"

else:
    msg += "_Nenhum_\n"

    msg += "\n⚠ **Inativos +10 dias**\n"

    if in10:
        for nome, dias in sorted(in10, key=lambda x: x[1], reverse=True):
            msg += f"{nome} — {dias} dias\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n❌ **Membros há mais de 20 dias sem tag 'Virtue' ou 'Culpa'**\n"

    if membros_sem_tag:
        for nome, dias, join_date in sorted(
            membros_sem_tag, key=lambda x: x[1], reverse=True
        ):
            data_str = join_date.strftime("%d/%m/%Y")
            tempo_txt = dias_para_tempo(dias)
            msg += f"{nome} — {tempo_txt}\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n🏆 **5 membros mais antigos da guilda**\n"

    for pos, (nome, data_entrada) in enumerate(antigos, start=1):
        tempo = datetime.now(BRASIL) - data_entrada
        dias = tempo.days
        anos = dias // 365
        meses = (dias % 365) // 30

        if anos > 0 and meses > 0:
            tempo_str = f"{anos} anos e {meses} meses"
        elif anos > 0:
            tempo_str = f"{anos} anos"
        elif meses > 0:
            tempo_str = f"{meses} meses"
        else:
            tempo_str = f"{dias} dias"

        medalha = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][pos - 1]

        msg += f"{medalha} {nome} — {tempo_str}\n"

    return msg


# =========================
# LOOP PRINCIPAL
# =========================
print("Bot auditoria iniciado")

while True:
    try:
        in20, in10, antigos, membros_sem_tag = analisar()
        msg = gerar_msg(in20, in10, antigos, membros_sem_tag)

        if mensagem_id:
            editar(msg)
        else:
            enviar(msg)

        print("Próxima análise em 24h")
        time.sleep(INTERVALO)

    except Exception as e:
        print("Erro:", e)
        time.sleep(60)
