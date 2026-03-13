import time
import json
import os
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# -----------------------
# CONFIG
# -----------------------

GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
WEBHOOK = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"

ARQUIVO_ESTADO = "estado_msg.json"
INTERVALO = 86400  # 24h
THREADS = 10

BRASIL = pytz.timezone("America/Sao_Paulo")

# -----------------------
# ESTADO
# -----------------------

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

# -----------------------
# DISCORD
# -----------------------

def enviar(msg):
    global mensagem_id
    r = requests.post(WEBHOOK + "?wait=true", json={"content": msg})
    if r.status_code in [200, 201]:
        mensagem_id = r.json()["id"]
        salvar_estado({"msg_id": mensagem_id})
        print("Mensagem criada no Discord")

def editar(msg):
    url = WEBHOOK + "/messages/" + mensagem_id
    requests.patch(url, json={"content": msg})
    print("Mensagem atualizada")

# -----------------------
# PEGAR MEMBROS COM SELENIUM
# -----------------------

def pegar_membros():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get(GUILD_URL)
    time.sleep(5)  # espera a página carregar JS

    membros = []
    guild_datas = {}

    try:
        # pega todas as linhas da tabela de membros
        linhas = driver.find_elements(By.CSS_SELECTOR, "table tr")
        for row in linhas[1:]:  # pula header
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 3:
                continue
            nome = cols[0].text.strip()
            join_text = cols[2].text.strip()
            try:
                data = datetime.strptime(join_text, "%b %d, %Y")
                data = BRASIL.localize(data)
                guild_datas[nome] = data
                membros.append(nome)
            except:
                continue
    finally:
        driver.quit()

    return membros, guild_datas

# -----------------------
# DETECTAR LAST ONLINE
# -----------------------

def last_online(nome):
    try:
        url = "https://www.rucoyonline.com/characters/" + nome.replace(" ", "%20")
        r = requests.get(url, timeout=10)
        texto = r.text.lower()
        if "currently online" in texto:
            return None
        match = re.search(r'last online\s*(\d+)\s*day', texto)
        if match: return int(match.group(1))
        match = re.search(r'last online\s*(\d+)\s*week', texto)
        if match: return int(match.group(1)) * 7
        match = re.search(r'last online\s*(\d+)\s*month', texto)
        if match: return int(match.group(1)) * 30
        match = re.search(r'last online\s*(\d+)\s*year', texto)
        if match: return int(match.group(1)) * 365
        return None
    except:
        return None

# -----------------------
# ANALISAR GUILDA
# -----------------------

def analisar():
    membros, guild_datas = pegar_membros()
    print(f"{len(membros)} membros encontrados")

    in20 = []
    in10 = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(last_online, m): m for m in membros}
        for future in as_completed(futures):
            nome = futures[future]
            dias = future.result()
            if dias is None:
                continue
            if dias >= 20:
                in20.append((nome, dias))
            elif dias >= 10:
                in10.append((nome, dias))

    # 5 membros mais antigos
    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]

    # membros há mais de 20 dias sem tag "Virtue" ou "Culpa"
    hoje = datetime.now(BRASIL)
    membros_sem_tag = []
    for nome, join_date in guild_datas.items():
        dias_na_guilda = (hoje - join_date).days
        if dias_na_guilda > 20 and "virtue" not in nome.lower() and "culpa" not in nome.lower():
            membros_sem_tag.append((nome, dias_na_guilda, join_date))

    return in20, in10, antigos, membros_sem_tag

# -----------------------
# GERAR MENSAGEM
# -----------------------

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
            msg += f"{nome} — {dias} dias\n"
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
        for nome, dias, join_date in sorted(membros_sem_tag, key=lambda x: x[1], reverse=True):
            data_str = join_date.strftime("%d/%m/%Y")
            msg += f"{nome} — {dias} dias — entrou em {data_str}\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n🏆 **5 membros mais antigos da guilda**\n"
    for pos, (nome, data_entrada) in enumerate(antigos, start=1):
        tempo = datetime.now(BRASIL) - data_entrada
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
# LOOP PRINCIPAL
# -----------------------

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
