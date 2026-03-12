import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------
# CONFIG
# -----------------------

GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
WEBHOOK = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"

ARQUIVO_ESTADO = "estado_msg.json"
ARQUIVO_MEMBROS = "membros_cache.json"

INTERVALO = 86400  # 24h
THREADS = 10       # quantos perfis verificar ao mesmo tempo

BRASIL = pytz.timezone("America/Sao_Paulo")

session = requests.Session()

# -----------------------
# ESTADO
# -----------------------

def salvar_estado(data):
    with open(ARQUIVO_ESTADO,"w") as f:
        json.dump(data,f)

def carregar_estado():
    if not os.path.exists(ARQUIVO_ESTADO):
        return {}
    with open(ARQUIVO_ESTADO,"r") as f:
        return json.load(f)

estado = carregar_estado()
mensagem_id = estado.get("msg_id")

def salvar_membros(lista):

    with open(ARQUIVO_MEMBROS,"w") as f:
        json.dump(lista,f)

def carregar_membros():

    if not os.path.exists(ARQUIVO_MEMBROS):
        return []

    with open(ARQUIVO_MEMBROS,"r") as f:
        return json.load(f)

# -----------------------
# DISCORD
# -----------------------

def enviar(msg):
    global mensagem_id

    r = requests.post(WEBHOOK+"?wait=true",json={"content":msg})

    if r.status_code in [200,201]:
        mensagem_id = r.json()["id"]
        salvar_estado({"msg_id":mensagem_id})
        print("Mensagem criada no Discord")

def editar(msg):
    url = WEBHOOK+"/messages/"+mensagem_id
    requests.patch(url,json={"content":msg})
    print("Mensagem atualizada")

# -----------------------
# PEGAR MEMBROS
# -----------------------

def pegar_membros():

    r = session.get(GUILD_URL)

    soup = BeautifulSoup(r.text,"html.parser")

    membros = []
    guild_datas = {}

    linhas = soup.select("table tr")

    for row in linhas:

        link = row.select_one("a[href*='/characters/']")
        cols = row.find_all("td")

        if not link or len(cols) < 3:
            continue

        nome = link.get_text(strip=True)

        join_text = cols[2].get_text(strip=True)

        try:
            data = datetime.strptime(join_text,"%b %d, %Y")
            data = BRASIL.localize(data)
            guild_datas[nome] = data
        except:
            pass

        membros.append(nome)

    return membros, guild_datas

# -----------------------
# DETECTAR LAST ONLINE
# -----------------------

def last_online(nome):

    url = "https://www.rucoyonline.com/characters/"+nome.replace(" ","%20")

    for tentativa in range(3):

        try:

            r = session.get(url,timeout=10)

            time.sleep(0.05)  # delay menor entre requests

            soup = BeautifulSoup(r.text,"html.parser")

            texto = soup.text.lower()

            if "currently online" in texto:
                return None

            match = re.search(r'last online\s*(\d+)\s*day',texto)
            if match:
                return int(match.group(1))

            match = re.search(r'last online\s*(\d+)\s*week',texto)
            if match:
                return int(match.group(1))*7

            match = re.search(r'last online\s*(\d+)\s*month',texto)
            if match:
                return int(match.group(1))*30

            match = re.search(r'last online\s*(\d+)\s*year',texto)
            if match:
                return int(match.group(1))*365

            return None

        except:
            time.sleep(1)

    return None

# ----------------------- # ANALISAR GUILDA (RÁPIDO) # -----------------------

def analisar():
    membros, guild_datas = pegar_membros()

    print(f"{len(membros)} membros encontrados")

    membros_antigos = carregar_membros()

    novos = [m for m in membros if m not in membros_antigos]
    saidos = [m for m in membros_antigos if m not in membros]

    salvar_membros(membros)

    in20=[]
    in10=[]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(last_online,m): m for m in membros}

        for future in as_completed(futures):
            nome = futures[future]
            dias = future.result()

            if dias is None:
                continue

            if dias >= 20:
                in20.append((nome,dias))
            elif dias >= 10:
                in10.append((nome,dias))

    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]

    # ⚠ aviso apenas, sem interromper o painel
    if len(in20) + len(in10) < 5:
        print("Aviso: poucos inativos detectados, mas painel será enviado mesmo assim")

    return in20, in10, antigos, novos, saidos

# ----------------------- # GERAR MENSAGEM # -----------------------

def gerar_msg(in20, in10, antigos, novos, saidos):
    agora = datetime.now(BRASIL)
    data = agora.strftime("%d/%m/%Y")
    hora = agora.strftime("%H:%M")

    msg = f"""📊 **Auditoria da Guilda**

🕒 Atualizado em: {data} às {hora} (Brasil)

❌ **Inativos +20 dias**
"""

    # inativos +20 dias
    if in20:
        for nome, dias in sorted(in20, key=lambda x: x[1], reverse=True):
            msg += f"{nome} — {dias} dias\n"
    else:
        msg += "_Nenhum_\n"

    # inativos +10 dias
    msg += "\n⚠ **Inativos +10 dias**\n"
    if in10:
        for nome, dias in sorted(in10, key=lambda x: x[1], reverse=True):
            msg += f"{nome} — {dias} dias\n"
    else:
        msg += "_Nenhum_\n"

    # novos membros
    if novos:
        msg += "\n🟢 **Entraram na guilda**\n"
        for n in novos:
            msg += f"{n}\n"

    # membros que saíram
    if saidos:
        msg += "\n🔴 **Saíram da guilda**\n"
        for s in saidos:
            msg += f"{s}\n"

    # 5 membros mais antigos
    msg += "\n🏆 **5 membros mais antigos da guilda**\n"
    for pos, (nome, data) in enumerate(antigos, start=1):
        tempo = datetime.now(BRASIL) - data
        dias = tempo.days
        anos = dias // 365
        meses = (dias % 365) // 30

        ano_txt = "ano" if anos == 1 else "anos"
        mes_txt = "mês" if meses == 1 else "meses"

        if anos > 0 and meses > 0:
            tempo_str = f"{anos} {ano_txt} e {meses} {mes_txt}"
        elif anos > 0:
            tempo_str = f"{anos} {ano_txt}"
        elif meses > 0:
            tempo_str = f"{meses} {mes_txt}"
        else:
            tempo_str = f"{dias} dias"

        if pos == 1:
            posicao = "🥇"
        elif pos == 2:
            posicao = "🥈"
        elif pos == 3:
            posicao = "🥉"
        else:
            posicao = f"{pos}️⃣"

        msg += f"{posicao} {nome} — {tempo_str}\n"

    return msg
    
# -----------------------
# LOOP
# -----------------------

print("Bot auditoria iniciado")

while True:
    try:
        resultado = analisar()
        if not resultado:
            time.sleep(60)
            continue

        in20, in10, antigos, novos, saidos = resultado
        msg = gerar_msg(in20, in10, antigos, novos, saidos)

        # Sempre criar nova mensagem no Discord no reinício
        if not mensagem_id:
            enviar(msg)
        else:
            editar(msg)

        print("Próxima análise em 24h")
        time.sleep(INTERVALO)

    except Exception as e:
        print("Erro:", e)
        time.sleep(60)
