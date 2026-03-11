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

    for link in soup.find_all("a",href=True):

        if "/characters/" in link["href"]:

            nome = link.text.strip()

            if nome and nome not in membros:
                membros.append(nome)

    return membros

# -----------------------
# DETECTAR LAST ONLINE
# -----------------------

def last_online(nome):

    try:

        url = "https://www.rucoyonline.com/characters/"+nome.replace(" ","%20")

        r = session.get(url,timeout=10)

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
        return None

# -----------------------
# ANALISAR GUILDA (RÁPIDO)
# -----------------------

def analisar():

    membros = pegar_membros()

    print(f"{len(membros)} membros encontrados")

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

    return in20,in10

# -----------------------
# GERAR MENSAGEM
# -----------------------

def gerar_msg(in20,in10):

    agora = datetime.now(BRASIL)

    data = agora.strftime("%d/%m/%Y")
    hora = agora.strftime("%H:%M")

    msg=f"""📊 **Auditoria da Guilda**

🕒 Atualizado em: {data} às {hora} (Brasil)

❌ **Inativos +20 dias**
"""

    if in20:
        for nome,dias in sorted(in20,key=lambda x:x[1],reverse=True):
            msg+=f"{nome} — {dias} dias\n"
    else:
        msg+="_Nenhum_\n"

    msg+="\n⚠ **Inativos +10 dias**\n"

    if in10:
        for nome,dias in sorted(in10,key=lambda x:x[1],reverse=True):
            msg+=f"{nome} — {dias} dias\n"
    else:
        msg+="_Nenhum_\n"

    return msg

# -----------------------
# LOOP
# -----------------------

print("Bot auditoria iniciado")

while True:

    try:

        in20,in10 = analisar()

        msg = gerar_msg(in20,in10)

        if mensagem_id:
            editar(msg)
        else:
            enviar(msg)

        print("Próxima análise em 24h")

        time.sleep(INTERVALO)

    except Exception as e:

        print("Erro:",e)

        time.sleep(60)
