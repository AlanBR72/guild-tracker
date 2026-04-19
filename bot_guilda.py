import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================  
GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
HUNTED_URL = "https://www.rucoyonline.com/guild/Peace%20Killers"
ARQUIVO_HUNTED = "hunted_data.json"
WEBHOOK = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"
WEBHOOK_RANK = "https://discord.com/api/webhooks/1494393213409300531/iX8kJAHYJdxQBZCGAOzb0vwC6HquvcfO6EZ2mFThwJ7phDDQbBqELMXcFW5t01P1rKYZ"

ARQUIVO_ESTADO = "estado_msg.json"
ARQUIVO_RANK = "rank_mage.json"
ARQUIVO_RANK_LEVEL = "rank_level.json"

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

def carregar_rank():
    if not os.path.exists(ARQUIVO_RANK):
        return {}

    with open(ARQUIVO_RANK, "r", encoding="utf-8") as f:
        return json.load(f)

def salvar_rank(data):
    with open(ARQUIVO_RANK, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def carregar_rank_level():
    if not os.path.exists(ARQUIVO_RANK_LEVEL):
        return {}

    with open(ARQUIVO_RANK_LEVEL, "r", encoding="utf-8") as f:
        return json.load(f)

def salvar_rank_level(data):
    with open(ARQUIVO_RANK_LEVEL, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def carregar_hunted():
    if not os.path.exists(ARQUIVO_HUNTED):
        return {"membros": [], "levels": {}}

    with open(ARQUIVO_HUNTED, "r", encoding="utf-8") as f:
        return json.load(f)


def salvar_hunted(data):
    with open(ARQUIVO_HUNTED, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =========================
# ESTADO (ID das msgs no Discord)
# =========================

estado = carregar_estado()

msg_id1 = estado.get("msg1")
msg_id2 = estado.get("msg2")
msg_id3 = estado.get("msg3")

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

    r = requests.post(WEBHOOK + "?wait=true", json={"content": msg})

    if r.status_code in (200, 201):

        msg_id = r.json()["id"]

        print("Mensagem criada no Discord")

        return msg_id

    else:
        print("Erro ao enviar mensagem:", r.text)
        return None

def enviar_rank(msg):

    r = requests.post(WEBHOOK_RANK + "?wait=true", json={"content": msg})

    if r.status_code in (200, 201):
        print("Rank enviado no Discord (bot 2)")
    else:
        print("Erro rank:", r.text)

def pegar_xp_dos_players(nomes):

    xp_dict = {}

    try:

        r = session.get(
            "https://www.rucoyonline.com/highscores/experience/2016/1",
            timeout=15
        )

        soup = BeautifulSoup(r.text, "html.parser")

        for row in soup.find_all("tr"):

            cols = row.find_all("td")

            if len(cols) >= 4:

                nome = cols[1].text.strip().replace("Online","").strip()

                if nome in nomes:

                    xp = int(cols[3].text.strip().replace(",", ""))

                    level = int(cols[2].text.strip())

                    xp_dict[nome] = {
                        "level": level,
                        "xp": xp
                    }

        return xp_dict

    except Exception as e:
        print("Erro ao pegar XP:", e)
        return {}

def top5_level_mage():

    jogadores = []

    try:

        r = session.get(
            "https://www.rucoyonline.com/highscores/magic/2016/1",
            timeout=15
        )

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.find_all("tr")[1:101]

        jogadores_magic = []

        for row in rows:

            cols = row.find_all("td")

            if len(cols) >= 3:

                nome = cols[1].text.strip().replace("Online","").strip()

                try:
                    magic = int(cols[2].text.strip())
                    jogadores_magic.append((nome, magic))
                except:
                    continue

        # 🔥 pega xp + level de todos
        nomes = [nome for nome, _ in jogadores_magic]
        xp_dict = pegar_xp_dos_players(nomes)

        for nome, magic in jogadores_magic:

            dados = xp_dict.get(nome)

            if dados:
                jogadores.append((
                    nome,
                    dados["level"],
                    magic,
                    dados["xp"]
                ))

        top5 = sorted(jogadores, key=lambda x: x[1], reverse=True)[:5]

        return [
            {
                "nome": nome,
                "level": level,
                "magic": magic,
                "xp": xp
            }
            for nome, level, magic, xp in top5
        ]

    except Exception as e:

        print("Erro rank mage:", e)
        return []

def gerar_msg_rank():

    top5 = top5_level_mage()
    rank_antigo = carregar_rank()

    agora = datetime.now(BRASIL)
    data = agora.strftime("%d/%m/%Y")
    hora = agora.strftime("%H:%M")

    msg = f"_🕒 Atualizado em: {data} • {hora}_\n\n"
    msg += "🏆 **TOP 5 LEVEL — MAGE (TOP 50)** 🏆\n\n"

    if not top5:
        msg += "_Erro ao carregar ranking_"
        return msg

    novo_rank = {}

    for i, player in enumerate(top5, 1):

        nome = player["nome"]
        level = player["level"]
        magic = player["magic"]
        xp = player.get("xp")

        medalha = ["🥇","🥈","🥉","4️⃣","5️⃣"][i-1]

        extra = ""

        if nome in rank_antigo:

            antigo = rank_antigo[nome]

            diff_level = level - antigo.get("level", 0)
            diff_magic = magic - antigo.get("magic", 0)
            diff_xp = 0

            if xp is not None and antigo.get("xp") is not None:
                diff_xp = xp - antigo.get("xp", 0)

            partes = []

            if diff_level > 0:
                partes.append(f"+{diff_level} lvl")

            if diff_magic > 0:
                partes.append(f"+{diff_magic} magic")

            if diff_xp >= 30_000_000:
                partes.append(f"+{int(diff_xp/1_000_000)}kk XP")

            if partes:
                extra = " (🆙 " + ", ".join(partes) + ")"

        msg += f"{medalha} _**{nome}** ➤ Level **{level}** | 🪄 Magic **{magic}**{extra}_\n"

        novo_rank[nome] = {
            "level": level,
            "magic": magic,
            "xp": xp
        }

    salvar_rank(novo_rank)

    return msg

def top7_level():

    try:

        r = session.get(
            "https://www.rucoyonline.com/highscores/experience/2016/1",
            timeout=15
        )

        soup = BeautifulSoup(r.text, "html.parser")

        jogadores = []

        for row in soup.find_all("tr")[1:51]:

            cols = row.find_all("td")

            if len(cols) >= 4:

                nome = cols[1].text.strip().replace("Online","").strip()
                level = int(cols[2].text.strip())
                xp = int(cols[3].text.strip().replace(",", ""))

                jogadores.append({
                    "nome": nome,
                    "level": level,
                    "xp": xp
                })

        # já vem ordenado, mas garantimos
        top7 = sorted(jogadores, key=lambda x: (x["level"], x["xp"]), reverse=True)[:7]

        return top7

    except Exception as e:
        print("Erro rank level:", e)
        return []

def gerar_msg_rank_level():

    msg = ""

    top7 = top7_level()
    rank_antigo = carregar_rank_level()  # vamos criar isso
    nomes = [p["nome"] for p in top7]
    skills = pegar_skills_players(nomes)

    msg += "🏆 **TOP 7 LEVEL GLOBAL** 🏆\n\n"

    if not top7:
        msg += "_Erro ao carregar ranking_"
        return msg

    novo_rank = {}

    for i, player in enumerate(top7, 1):

        nome = player["nome"]
        level = player["level"]
        xp = player["xp"]

        skill = skills.get(nome, {"melee":0,"distance":0,"magic":0})

        medalha = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣"][i-1]

        extra = ""

        if nome in rank_antigo:

            antigo = rank_antigo[nome]

            diff_level = level - antigo.get("level", 0)
            diff_xp = xp - antigo.get("xp", 0)
            skill_antigo = rank_antigo.get(nome, {})
            diff_melee = skill["melee"] - skill_antigo.get("melee", 0)
            diff_dist = skill["distance"] - skill_antigo.get("distance", 0)
            diff_magic = skill["magic"] - skill_antigo.get("magic", 0)

            partes = []

            if diff_level > 0:
                partes.append(f"+{diff_level} lvl")

            if diff_xp >= 30_000_000:
                partes.append(f"+{int(diff_xp/1_000_000)}kk XP")

            if diff_melee > 0:
                partes.append(f"+{diff_melee} melee")

            if diff_dist > 0:
                partes.append(f"+{diff_dist} dist")

            if diff_magic > 0:
                partes.append(f"+{diff_magic} magic")

            if partes:
                extra = " (🆙 " + ", ".join(partes) + ")"

        emoji, valor_skill = detectar_classe(skill)

        msg += f"{medalha} _**{nome}** ➤ Level **{level}** | {emoji} **{valor_skill}**{extra}_\n"

        novo_rank[nome] = {
            "level": level,
            "xp": xp,
            "melee": skill["melee"],
            "distance": skill["distance"],
            "magic": skill["magic"]
        }

    salvar_rank_level(novo_rank)

    return msg

def pegar_skills_players(nomes):

    skills = {nome: {"melee": 0, "distance": 0, "magic": 0} for nome in nomes}

    try:
        # -----------------------
        # MELEE
        # -----------------------
        r = session.get("https://www.rucoyonline.com/highscores/melee/2016/1", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 3:
                nome = cols[1].text.strip().replace("Online","").strip()
                if nome in skills:
                    skills[nome]["melee"] = int(cols[2].text.strip())

        # -----------------------
        # DISTANCE
        # -----------------------
        r = session.get("https://www.rucoyonline.com/highscores/distance/2016/1", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 3:
                nome = cols[1].text.strip().replace("Online","").strip()
                if nome in skills:
                    skills[nome]["distance"] = int(cols[2].text.strip())

        # -----------------------
        # MAGIC
        # -----------------------
        r = session.get("https://www.rucoyonline.com/highscores/magic/2016/1", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 3:
                nome = cols[1].text.strip().replace("Online","").strip()
                if nome in skills:
                    skills[nome]["magic"] = int(cols[2].text.strip())

        return skills

    except Exception as e:
        print("Erro ao pegar skills:", e)
        return {}

def detectar_classe(skill):

    melee = skill["melee"]
    dist = skill["distance"]
    magic = skill["magic"]

    if melee >= dist and melee >= magic:
        return "⚔️ Melee", melee

    elif dist >= melee and dist >= magic:
        return "🏹 Dist", dist

    else:
        return "🪄 Magic", magic

def pegar_membros_hunted():

    r = session.get(HUNTED_URL, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")

    membros = []
    levels = {}

    tabela = soup.select_one("table")

    if not tabela:
        return membros, levels

    for row in tabela.find_all("tr")[1:]:

        cols = row.find_all("td")

        if len(cols) < 3:
            continue

        link = row.select_one("a[href*='/characters/']")
        if not link:
            continue

        nome = link.get_text(strip=True)

        try:
            level = int(cols[1].get_text(strip=True))
        except:
            continue

        membros.append(nome)
        levels[nome] = level

    return membros, levels

def analisar_hunted():

    membros, levels_atuais = pegar_membros_hunted()
    dados_antigos = carregar_hunted()

    membros_antigos = dados_antigos.get("membros", [])
    levels_antigos = dados_antigos.get("levels", {})

    # =========================
    # ENTRADAS / SAÍDAS
    # =========================
    entraram = list(set(membros) - set(membros_antigos))
    sairam = list(set(membros_antigos) - set(membros))

    # =========================
    # UPS / DOWNS
    # =========================
    ups = []
    downs = []

    for nome, level in levels_atuais.items():

        if nome in levels_antigos:

            antigo = levels_antigos[nome]
            diff = level - antigo

            if diff > 0:
                ups.append((nome, diff))
            elif diff < 0:
                downs.append((nome, abs(diff)))

    # =========================
    # ESTATÍSTICAS
    # =========================
    total = len(levels_atuais)

    media = round(sum(levels_atuais.values()) / total) if total > 0 else 0

    l600 = sum(1 for l in levels_atuais.values() if l >= 600)
    l700 = sum(1 for l in levels_atuais.values() if l >= 700)
    l800 = sum(1 for l in levels_atuais.values() if l >= 800)

    # =========================
    # SALVAR NOVO ESTADO
    # =========================
    salvar_hunted({
        "membros": membros,
        "levels": levels_atuais
    })

    return total, media, l600, l700, l800, entraram, sairam, ups, downs

def gerar_msg_hunted():

    msg = ""

    total, media, l600, l700, l800, entraram, sairam, ups, downs = analisar_hunted()

    msg += "🔥 **RELATÓRIO — PEACE KILLERS (HUNTED)** 🔥\n\n"

    msg += f"👥 **Membros:** {total}\n"
    msg += f"⚔️ **Média de level:** {media}\n\n"

    msg += "📊 **Distribuição de força**\n"
    msg += f"_Level 600+ ➤ {l600}_\n"
    msg += f"_Level 700+ ➤ {l700}_\n"
    msg += f"_Level 800+ ➤ {l800}_\n\n"

    msg += "📥 **Entraram**\n"
    msg += "\n".join(f"_{n}_" for n in entraram) if entraram else "_Nenhum_"

    msg += "\n\n📤 **Saíram**\n"
    msg += "\n".join(f"_{n}_" for n in sairam) if sairam else "_Nenhum_"

    msg += "\n\n📈 **Ups de level**\n"
    msg += "\n".join(f"_{n} (+{d})_" for n, d in ups) if ups else "_Nenhum_"

    msg += "\n\n📉 **Downs de level**\n"
    msg += "\n".join(f"_{n} (-{d})_" for n, d in downs) if downs else "_Nenhum_"

    return msg

def segundos_ate_proximo_horario(hora_alvo=3):

    agora = datetime.now(BRASIL)

    alvo = agora.replace(hour=hora_alvo, minute=0, second=0, microsecond=0)

    # se já passou das 03h hoje → agenda pra amanhã
    if agora >= alvo:
        alvo += timedelta(days=1)

    return (alvo - agora).total_seconds()

def editar(msg_id, msg):

    if not msg_id:
        return enviar(msg)

    url = WEBHOOK + "/messages/" + msg_id

    r = requests.patch(url, json={"content": msg})

    if r.status_code in (200, 204):
        print("Mensagem atualizada")
        return msg_id

    else:
        print("Erro ao editar mensagem:", r.text)
        return msg_id

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

    media_level = round(sum(levels_atuais.values()) / len(levels_atuais))

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
    # QUASE LEVEL (600 / 700 / 800)
    # =========================

    quase_levels = []

    for nome, level in levels_atuais.items():

        for alvo in [600, 700, 800]:

            faltam = alvo - level

            if 0 < faltam <= 5:
                quase_levels.append((nome, level, alvo, faltam))

    quase_levels = sorted(quase_levels, key=lambda x: x[1], reverse=True)

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

    # =========================
    # RETURN FINAL
    # =========================

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
        forca_guilda,
        len(membros),
        media_level,
        quase_levels
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
    
def gerar_msg(in20, in10, antigos, membros_sem_tag, entraram, sairam, level_ups, level_downs, distribuicao, top_levels, forca_guilda, total_membros, media_level, quase_levels):

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

    msg1 += "\n🎯 **Quase level importante**\n"

    if quase_levels:

        for nome, level, alvo, faltam in quase_levels:

            msg1 += f"_{nome} ➤ {level} (faltam {faltam} para {alvo})_\n"

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
    msg3 += f"👥 **Membros:** {total_membros}\n"
    msg3 += f"💪 **Força da Guilda:** _{forca_txt}_\n"
    msg3 += f"⚔️ **Média de level da guilda:** _{media_level}_\n\n"

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
    msg3 += f"_Level 800+ ➤ {distribuicao['800-899']} membros_\n"
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

        in20, in10, antigos, membros_sem_tag, entraram, sairam, level_ups, level_downs, distribuicao, top_levels, forca_guilda, total_membros, media_level, quase_levels = analisar()

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
            forca_guilda,
            total_membros,
            media_level,
            quase_levels
        )

        if msg_id1:

            msg_id1 = editar(msg_id1, msg1)
            msg_id2 = editar(msg_id2, msg2)
            msg_id3 = editar(msg_id3, msg3)

        else:

            msg_id1 = enviar(msg1)
            msg_id2 = enviar(msg2)
            msg_id3 = enviar(msg3)

            salvar_estado({
                "msg1": msg_id1,
                "msg2": msg_id2,
                "msg3": msg_id3
            })

        # =========================
        # RANK/TRACK (SEGUNDO BOT)
        # =========================

        print("🏆 Gerando painel completo...")

        msg_rank = gerar_msg_rank()
        msg_rank_level = gerar_msg_rank_level()
        msg_hunted = gerar_msg_hunted()

        msg_final = (
            msg_rank
            + "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + msg_rank_level
            + "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + msg_hunted
        )

        enviar_rank(msg_final)

        print("Próxima análise em 24h")
        time.sleep(INTERVALO)

    except Exception as e:
        print("Erro:", e)
        time.sleep(60)
