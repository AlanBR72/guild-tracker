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

def salvar_membros(lista):
    with open("membros_guilda.json", "w", encoding="utf-8") as f:
        json.dump(lista, f, ensure_ascii=False, indent=2)

def carregar_membros():
    if not os.path.exists("membros_guilda.json"):
        return None

    with open("membros_guilda.json", "r", encoding="utf-8") as f:
        return json.load(f)

def salvar_levels(levels):
    with open("levels_guilda.json", "w", encoding="utf-8") as f:
        json.dump(levels, f, ensure_ascii=False, indent=2)

def carregar_levels():
    if not os.path.exists("levels_guilda.json"):
        return None

    with open("levels_guilda.json", "r", encoding="utf-8") as f:
        return json.load(f)

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
    levels = {}

    # tabela da guilda
    tabela = soup.select_one("table")

    if not tabela:
        return membros, guild_datas, levels

    linhas = tabela.select("tr")

    for row in linhas[1:]:
        cols = row.find_all("td")

        if len(cols) < 3:
            continue

        link = row.select_one("a[href*='/characters/']")

        if not link:
            continue

        nome = link.get_text(strip=True)

        # =========================
        # LEVEL
        # =========================
        level_text = cols[1].get_text(strip=True)

        try:
            level = int(level_text)
        except:
            continue

        # =========================
        # DATA DE ENTRADA
        # =========================
        join_text = cols[2].get_text(strip=True)

        try:
            join_date = datetime.strptime(join_text, "%b %d, %Y")
            join_date = BRASIL.localize(join_date)
        except:
            continue

        membros.append(nome)
        guild_datas[nome] = join_date
        levels[nome] = level

    return membros, guild_datas, levels

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

    membros, guild_datas, levels_atuais = pegar_membros()
    print(f"{len(membros)} membros encontrados")

    # =========================
    # ESTATÍSTICAS DE LEVEL
    # =========================

    distribuicao = {
        "800-899": 0,
        "700-799": 0,
        "600-699": 0,
        "500-599": 0
    }

    for level in levels_atuais.values():

        if level >= 800:
            distribuicao["800-899"] += 1

        elif level >= 700:
            distribuicao["700-799"] += 1

        elif level >= 600:
            distribuicao["600-699"] += 1

        elif level >= 500:
            distribuicao["500-599"] += 1

    top_levels = sorted(levels_atuais.items(), key=lambda x: x[1], reverse=True)[:5]

    forca_guilda = sum(levels_atuais.values())

    # =========================
    # DETECTAR ENTRADAS / SAÍDAS
    # =========================

    membros_atuais = membros
    membros_antigos = carregar_membros()

    entraram = []
    sairam = []

    if membros_antigos is not None:
        entraram = list(set(membros_atuais) - set(membros_antigos))
        sairam = list(set(membros_antigos) - set(membros_atuais))

    salvar_membros(membros_atuais)

    # =========================
    # DETECTAR LEVEL UPS / DOWNS
    # =========================

    levels_antigos = carregar_levels()

    level_ups = []
    level_downs = []

    if levels_antigos:

        for nome, level in levels_atuais.items():

            if nome in levels_antigos:

                antigo = levels_antigos[nome]
                diff = level - antigo

                if diff > 0:
                    level_ups.append((nome, antigo, level, diff))

                elif diff < 0:
                    level_downs.append((nome, antigo, level, abs(diff)))

    salvar_levels(levels_atuais)

    # =========================
    # INATIVOS
    # =========================

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

    # =========================
    # MEMBROS MAIS ANTIGOS
    # =========================

    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]

    # =========================
    # MEMBROS SEM TAG
    # =========================

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

    return (
        in20,
        in10,
        antigos,
        membros_sem_tag,
        entraram,
        sairam,
        level_ups,
        level_downs,
        distribuicao,
        top_levels,
        forca_guilda
    )
    
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

# =========================
# FORMATAR NUMERO EM K
# =========================
def formatar_k(valor):

    if valor >= 1000:
        return f"{valor/1000:.1f}k"
    else:
        return str(valor)
    
def gerar_msg(in20, in10, antigos, membros_sem_tag, entraram, sairam, level_ups, level_downs, distribuicao, top_levels, forca_guilda):

    agora = datetime.now(BRASIL)
    data = agora.strftime("%d/%m/%Y")
    hora = agora.strftime("%H:%M")

    msg1 = f"""_🕒 Atualizado em: {data} • {hora} (Brasil)_"""
    msg2 = ""
    msg3 = ""

    # =========================
    # ENTRARAM / SAÍRAM
    # =========================

    msg1 += "\n\n📊 ═══════ **AUDITORIA DA GUILDA** ═══════ 📊"
    msg1 += "\n\n📥 **Entraram na guilda**\n"

    if entraram:
        for nome in sorted(entraram):
            msg1 += f"_{nome}_\n"
    else:
        msg1 += "_Nenhum_\n"

    msg1 += "\n📤 **Saíram da guilda**\n"

    if sairam:
        for nome in sorted(sairam):
            msg1 += f"_{nome}_\n"
    else:
        msg1 += "_Nenhum_\n"

    # =========================
    # LEVEL UPS
    # =========================

    msg1 += "\n📈 **Level ups da guilda**\n"

    if level_ups:
        for nome, antigo, novo, diff in sorted(level_ups, key=lambda x: x[3], reverse=True):
            msg1 += f"_{nome} ➤ {antigo} → {novo} (+{diff})_\n"
    else:
        msg1 += "_Nenhum_\n"

    # =========================
    # LEVEL DOWNS
    # =========================

    msg1 += "\n📉 **Level down da guilda**\n"

    if level_downs:
        for nome, antigo, novo, diff in sorted(level_downs, key=lambda x: x[3], reverse=True):
            msg1 += f"_{nome} ➤ {antigo} → {novo} (-{diff})_\n"
    else:
        msg1 += "_Nenhum_\n"

    # =========================
    # INATIVOS
    # =========================

    msg2 += "**🚫 ═══════ MEMBROS INATIVOS ═══════ 🚫**\n\n"
    msg2 += "🚫 **Inativos há mais de 20 dias**\n"

    if in20:
        for nome, dias in sorted(in20, key=lambda x: x[1], reverse=True):

            if dias >= 30:
                dias_txt = "30+ dias"
            else:
                dias_txt = f"{dias} dias"

            msg2 += f"_{nome} ➤ {dias_txt}_\n"
    else:
        msg2 += "_Nenhum_\n"

    msg2 += "\n⚠️ **Inativos há mais de 10 dias**\n"

    if in10:
        for nome, dias in sorted(in10, key=lambda x: x[1], reverse=True):
            msg2 += f"_{nome} ➤ {dias} dias_\n"
    else:
        msg2 += "_Nenhum_\n"

    # =========================
    # SEM TAG
    # =========================

    msg2 += "\n\n**🏷️ ═══════ MEMBROS SEM TAG ═══════ 🏷️**\n\n"
    msg2 += "❌ **Membros há mais de 20 dias sem tag (Virtue / Culpa):**\n"

    if membros_sem_tag:
        for nome, dias, join_date in sorted(membros_sem_tag, key=lambda x: x[1], reverse=True):
            tempo_txt = dias_para_tempo(dias)
            msg2 += f"_{nome} ➤ {tempo_txt}_\n"
    else:
        msg2 += "_Nenhum_\n"

    # =========================
    # ESTATÍSTICAS
    # =========================

    forca_txt = formatar_k(forca_guilda)

    msg3 += "**🏆 ═══════ ESTATÍSTICAS DA GUILDA ═══════ 🏆**\n\n"
    msg3 += f"👥 **Membros:** {len(membros)}\n"
    msg3 += f"💪 **Força da Guilda:** _{forca_txt}_\n\n"

    msg3 += "🏆 **Top 5 maiores levels da guilda**\n"

    for pos, (nome, level) in enumerate(top_levels, start=1):

        medalha = ["🔥","🥈","🥉","4️⃣","5️⃣"][pos-1]
        msg3 += f"{medalha} _{nome} ➤ level {level}_\n"

    # =========================
    # MAIS ANTIGOS
    # =========================

    msg3 += "\n👴 **5 Membros mais antigos da guilda:**\n"

    for pos, (nome, data_entrada) in enumerate(antigos, start=1):

        tempo = datetime.now(BRASIL) - data_entrada
        dias = tempo.days
        anos = dias // 365
        meses = (dias % 365) // 30

        if anos == 1:
            ano_txt = "1 ano"
        elif anos > 1:
            ano_txt = f"{anos} anos"
        else:
            ano_txt = ""

        if meses == 1:
            mes_txt = "1 mês"
        elif meses > 1:
            mes_txt = f"{meses} meses"
        else:
            mes_txt = ""

        if ano_txt and mes_txt:
            tempo_str = f"{ano_txt} e {mes_txt}"
        elif ano_txt:
            tempo_str = ano_txt
        elif mes_txt:
            tempo_str = mes_txt
        else:
            tempo_str = f"{dias} dias"

        medalha = ["🥇", "🥈", "🥉", "🎖️", "🏅"][pos - 1]

        msg3 += f"{medalha} _{nome} ➤ {tempo_str}_\n"

    # =========================
    # DISTRIBUIÇÃO
    # =========================

    msg3 += "\n📊 **Distribuição de levels**\n"
    msg3 += f"_Level 800-899 ➤ {distribuicao['800-899']} membros_\n"
    msg3 += f"_Level 700-799 ➤ {distribuicao['700-799']} membros_\n"
    msg3 += f"_Level 600-699 ➤ {distribuicao['600-699']} membros_\n"
    msg3 += f"_Level 500-599 ➤ {distribuicao['500-599']} membros_\n"

    return msg1, msg2, msg3
    
# =========================
# LOOP PRINCIPAL
# =========================
print("Bot auditoria iniciado")

while True:
    try:
        in20, in10, antigos, membros_sem_tag, entraram, sairam, level_ups, level_downs, distribuicao, top_levels, forca_guilda = analisar()

        msg1, msg2, msg3 = gerar_msg(
            in20,
            in10,
            antigos,
            membros_sem_tag,
            entraram,
            sairam,
            level_ups,
            level_downs,
            distribuicao,
            top_levels,
            forca_guilda
        )

        if mensagem_id:
            editar(msg1)
            editar(msg2)
            editar(msg3)
        else:
            enviar(msg1)
            enviar(msg2)
            enviar(msg3)

        print("Próxima análise em 24h")
        time.sleep(INTERVALO)

    except Exception as e:
        print("Erro:", e)
        time.sleep(60)
