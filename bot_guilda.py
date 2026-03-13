import time
import json
import os
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re
from bs4 import BeautifulSoup

# -----------------------
# CONFIGURAÇÃO
# -----------------------
GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
WEBHOOK = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"  # Coloque seu webhook

ARQUIVO_ESTADO = "estado_msg.json"
INTERVALO = 86400  # 24h
THREADS = 10

BRASIL = pytz.timezone("America/Sao_Paulo")
session = requests.Session()

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
# PEGAR MEMBROS DA GUILDA
# -----------------------
def pegar_membros():
    r = session.get(GUILD_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    membros = []
    guild_datas = {}
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

# -----------------------
# PEGAR LAST ONLINE (SÓ SE NECESSÁRIO)
# -----------------------
def last_online_requests(nome):
    try:
        url = f"https://www.rucoyonline.com/characters/{nome.replace(' ','%20')}"
        r = session.get(url, timeout=10)
        texto = r.text.lower()

        if "currently online" in texto:
            return None

        patterns = [
            (r'last online\s*(\d+)\s*day',1),
            (r'last online\s*(\d+)\s*days',1),
            (r'last online\s*(\d+)\s*week',7),
            (r'last online\s*(\d+)\s*weeks',7),
            (r'last online\s*(\d+)\s*month',30),
            (r'last online\s*(\d+)\s*months',30),
            (r'last online\s*(\d+)\s*year',365),
            (r'last online\s*(\d+)\s*years',365),
        ]
        for pattern, mult in patterns:
            match = re.search(pattern, texto)
            if match:
                return int(match.group(1)) * mult
        return None
    except Exception as e:
        print(f"Erro ao pegar {nome}: {e}")
        return None

# -----------------------
# ANALISAR GUILDA OTIMIZADO
# -----------------------
def analisar():
    membros, guild_datas = pegar_membros()
    print(f"{len(membros)} membros encontrados")

    in20=[]
    in10=[]

    # Filtra apenas os membros que podem estar inativos
    # Você pode adicionar lógica de "últimos logins recentes" se quiser
    perfis_para_verificar = membros  # aqui verifica todos, mas você pode filtrar

    # ThreadPoolExecutor só acessa perfis realmente necessários
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(last_online_requests, m): m for m in perfis_para_verificar}
        for future in as_completed(futures):
            nome = futures[future]
            dias = future.result()
            if dias is None:
                continue
            if dias >= 20:
                in20.append((nome,dias))
            elif dias >= 10:
                in10.append((nome,dias))

    # 5 membros mais antigos
    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]

    # membros sem tag +20 dias
    hoje = datetime.now(BRASIL)
    membros_sem_tag=[]
    for nome, join_date in guild_datas.items():
        dias_na_guilda = (hoje - join_date).days
        if dias_na_guilda > 20 and "virtue" not in nome.lower() and "culpa" not in nome.lower():
            membros_sem_tag.append((nome,dias_na_guilda,join_date))

    return in20, in10, antigos, membros_sem_tag

# -----------------------
# GERAR MENSAGEM
# -----------------------
def gerar_msg(in20,in10,antigos,membros_sem_tag):
    agora=datetime.now(BRASIL)
    data=agora.strftime("%d/%m/%Y")
    hora=agora.strftime("%H:%M")

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

    msg+="\n❌ **Membros há mais de 20 dias sem tag 'Virtue' ou 'Culpa'**\n"
    if membros_sem_tag:
        for nome,dias,join_date in sorted(membros_sem_tag,key=lambda x:x[1],reverse=True):
            data_str = join_date.strftime("%d/%m/%Y")
            msg+=f"{nome} — {dias} dias — entrou em {data_str}\n"
    else:
        msg+="_Nenhum_\n"

    msg+="\n🏆 **5 membros mais antigos da guilda**\n"
    for pos,(nome,data_entrada) in enumerate(antigos,start=1):
        tempo=datetime.now(BRASIL)-data_entrada
        dias=tempo.days
        anos=dias//365
        meses=(dias%365)//30
        ano_txt="ano" if anos==1 else "anos"
        mes_txt="mês" if meses==1 else "meses"
        if anos>0 and meses>0:
            tempo_str=f"{anos} {ano_txt} e {meses} {mes_txt}"
        elif anos>0:
            tempo_str=f"{anos} {ano_txt}"
        elif meses>0:
            tempo_str=f"{meses} {mes_txt}"
        else:
            tempo_str=f"{dias} dias"

        if pos==1:
            posicao="🥇"
        elif pos==2:
            posicao="🥈"
        elif pos==3:
            posicao="🥉"
        else:
            posicao=f"{pos}️⃣"

        msg+=f"{posicao} {nome} — {tempo_str}\n"
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
        print("Erro:",e)
        time.sleep(60)
