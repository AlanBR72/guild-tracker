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

INTERVALO = 86400
THREADS = 10

BRASIL = pytz.timezone("America/Sao_Paulo")

session = requests.Session()

# -----------------------
# ESTADO DISCORD
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
# CACHE MEMBROS
# -----------------------

def salvar_cache(membros):
    with open(ARQUIVO_MEMBROS,"w") as f:
        json.dump({"membros": membros},f)

def carregar_cache():
    if not os.path.exists(ARQUIVO_MEMBROS):
        return []
    with open(ARQUIVO_MEMBROS,"r") as f:
        data = json.load(f)
        return data.get("membros",[])

# -----------------------
# DISCORD
# -----------------------

def enviar(msg):
    global mensagem_id
    r = requests.post(WEBHOOK+"?wait=true",json={"content":msg})
    if r.status_code in [200,201]:
        mensagem_id = r.json()["id"]
        salvar_estado({"msg_id":mensagem_id})

def editar(msg):
    url = WEBHOOK+"/messages/"+mensagem_id
    requests.patch(url,json={"content":msg})

# -----------------------
# TEMPO NA GUILDA
# -----------------------

def formatar_tempo(data_entrada):
    agora = datetime.now(BRASIL)
    dias = (agora - data_entrada).days

    if dias < 30:
        return f"{dias} dias"

    elif dias < 365:
        meses = dias // 30
        return f"{meses} meses"

    else:
        anos = dias // 365
        meses = (dias % 365) // 30

        if meses == 0:
            return f"{anos} anos"
        else:
            return f"{anos} anos e {meses} meses"

# -----------------------
# PEGAR MEMBROS
# -----------------------

def pegar_membros():
    r = session.get(GUILD_URL)
    soup = BeautifulSoup(r.text,"html.parser")

    membros = {}

    tabela = soup.find_all("tr")

    for row in tabela:

        col = row.find_all("td")

        if len(col) >= 3:

            nome = col[0].text.strip()

            data = col[2].text.strip()

            try:
                data_entrada = datetime.strptime(data,"%d/%m/%Y")
                data_entrada = BRASIL.localize(data_entrada)
            except:
                continue

            membros[nome] = data_entrada

    return membros

# -----------------------
# LAST ONLINE
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
# ANALISAR
# -----------------------

def analisar():

    membros = pegar_membros()

    in20=[]
    in10=[]
    sem_tag=[]
    antigos=[]

    resultados={}

    with ThreadPoolExecutor(max_workers=THREADS) as executor:

        futures={executor.submit(last_online,n):n for n in membros}

        for future in as_completed(futures):

            nome=futures[future]

            dias=future.result()

            resultados[nome]=dias

            if dias is not None:

                if dias>=20:
                    in20.append((nome,dias))

                elif dias>=10:
                    in10.append((nome,dias))

            # verificar sem TAG +20 dias guilda

            if not ("culpa" in nome.lower() or "virtue" in nome.lower()):

                dias_guilda=(datetime.now(BRASIL)-membros[nome]).days

                if dias_guilda>=20:

                    sem_tag.append((nome,formatar_tempo(membros[nome])))

    # top antigos

    membros_ordenados=sorted(membros.items(),key=lambda x:x[1])

    top5=membros_ordenados[:5]

    top5_formatado=[(nome,formatar_tempo(data)) for nome,data in top5]

    return in20,in10,sem_tag,top5_formatado,list(membros.keys())

# -----------------------
# GERAR MSG
# -----------------------

def gerar_msg(in20,in10,sem_tag,top5,novos,saidos):

    agora=datetime.now(BRASIL)

    data=agora.strftime("%d/%m/%Y")
    hora=agora.strftime("%H:%M")

    msg=f"📊 **Auditoria da Guilda**\n\n🕒 Atualizado em: {data} às {hora} (Brasil)\n\n"

    if novos:

        msg+="🟢 **Novos membros detectados**\n"

        for n in novos:
            msg+=f"{n}\n"

        msg+="\n"

    if saidos:

        msg+="🔴 **Membros que saíram da guilda**\n"

        for s in saidos:
            msg+=f"{s}\n"

        msg+="\n"

    msg+="❌ **Inativos +20 dias**\n"

    if in20:
        for n,d in sorted(in20,key=lambda x:x[1],reverse=True):
            msg+=f"{n} — {d} dias\n"
    else:
        msg+="_Nenhum_\n"

    msg+="\n⚠ **Inativos +10 dias**\n"

    if in10:
        for n,d in sorted(in10,key=lambda x:x[1],reverse=True):
            msg+=f"{n} — {d} dias\n"
    else:
        msg+="_Nenhum_\n"

    msg+="\n🚫 **Sem TAG 'Culpa' ou 'Virtue' (+20 dias na guilda)**\n"

    if sem_tag:
        for n,t in sem_tag:
            msg+=f"{n} — {t}\n"
    else:
        msg+="_Nenhum_\n"

    msg+="\n🏆 **5 membros mais antigos da guilda**\n"

    for n,t in top5:
        msg+=f"{n} — {t}\n"

    return msg

# -----------------------
# LOOP
# -----------------------

print("Bot iniciado")

while True:

    try:

        in20,in10,sem_tag,top5,membros_atuais=analisar()

        membros_antigos=carregar_cache()

        novos=[m for m in membros_atuais if m not in membros_antigos]

        saidos=[m for m in membros_antigos if m not in membros_atuais]

        msg=gerar_msg(in20,in10,sem_tag,top5,novos,saidos)

        if mensagem_id:
            editar(msg)
        else:
            enviar(msg)

        salvar_cache(membros_atuais)

        time.sleep(INTERVALO)

    except Exception as e:

        print("Erro:",e)

        time.sleep(60)
