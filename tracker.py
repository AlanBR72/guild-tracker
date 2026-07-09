import requests
from bs4 import BeautifulSoup

from config import (
    ARQUIVO_HUNTED,
    ARQUIVO_HISTORICO_MOB_XP_PEACE,
    ARQUIVO_MOB_XP_PEACE_LEVELS,
    ARQUIVO_RANK,
    ARQUIVO_RANK_LEVEL,
    HIGHSCORE_DISTANCE,
    HIGHSCORE_MAGIC,
    HIGHSCORE_MELEE,
    HIGHSCORE_XP,
    HUNTED_URL,
    ARQUIVO_ESTADO,
    DISCORD_LIMITE,
    REQUEST_TIMEOUT,
    USER_AGENT,
)
from discord_utils import atualizar_painel, enviar, enviar_em_partes
from storage import carregar_json, salvar_json
from utils import data_hora_brasil, data_hora_segundos_brasil, detectar_classe

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def soup_get(url):
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    return BeautifulSoup(r.text, "html.parser")


# =========================
# XP / SKILLS
# =========================
def pegar_xp_dos_players(nomes):
    xp_dict = {}
    try:
        soup = soup_get(HIGHSCORE_XP)
        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 4:
                nome = cols[1].text.strip().replace("Online", "").strip()
                if nome in nomes:
                    xp_dict[nome] = {
                        "level": int(cols[2].text.strip()),
                        "xp": int(cols[3].text.strip().replace(",", "")),
                    }
    except Exception as e:
        print(f"[tracker] erro ao pegar XP: {e}")
    return xp_dict


def pegar_skills_players(nomes):
    skills = {nome: {"melee": 0, "distance": 0, "magic": 0} for nome in nomes}
    fontes = [
        ("melee", HIGHSCORE_MELEE),
        ("distance", HIGHSCORE_DISTANCE),
        ("magic", HIGHSCORE_MAGIC),
    ]

    for skill_name, url in fontes:
        try:
            soup = soup_get(url)
            for row in soup.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 3:
                    nome = cols[1].text.strip().replace("Online", "").strip()
                    if nome in skills:
                        skills[nome][skill_name] = int(cols[2].text.strip())
        except Exception as e:
            print(f"[tracker] erro ao pegar {skill_name}: {e}")

    return skills


# =========================
# TOP MAGE
# =========================
def top5_level_mage():
    jogadores = []
    try:
        soup = soup_get(HIGHSCORE_MAGIC)
        rows = soup.find_all("tr")[1:101]
        jogadores_magic = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                nome = cols[1].text.strip().replace("Online", "").strip()
                try:
                    magic = int(cols[2].text.strip())
                    jogadores_magic.append((nome, magic))
                except Exception:
                    continue

        nomes = [nome for nome, _ in jogadores_magic]
        xp_dict = pegar_xp_dos_players(nomes)

        for nome, magic in jogadores_magic:
            dados = xp_dict.get(nome)
            if dados:
                jogadores.append((nome, dados["level"], magic, dados["xp"]))

        top5 = sorted(jogadores, key=lambda x: x[1], reverse=True)[:5]
        return [{"nome": n, "level": l, "magic": m, "xp": xp} for n, l, m, xp in top5]
    except Exception as e:
        print(f"[tracker] erro rank mage: {e}")
        return []


def gerar_msg_rank_mage():
    top5 = top5_level_mage()
    rank_antigo = carregar_json(ARQUIVO_RANK, {})
    data, hora = data_hora_brasil()

    msg = f"_🕒 Atualizado em: {data} • {hora}_\n\n"
    msg += "🧙‍♂️ **TOP 5 LEVEL — MAGE (TOP 100 MAGIC)** 🧙‍♂️\n\n"

    if not top5:
        return msg + "_Erro ao carregar ranking_"

    novo_rank = {}
    for i, player in enumerate(top5, 1):
        nome = player["nome"]
        level = player["level"]
        magic = player["magic"]
        xp = player.get("xp")
        medalha = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][i - 1]
        extra = ""

        if nome in rank_antigo:
            antigo = rank_antigo[nome]
            partes = []
            diff_level = level - antigo.get("level", 0)
            diff_magic = magic - antigo.get("magic", 0)
            diff_xp = xp - antigo.get("xp", 0) if xp is not None and antigo.get("xp") is not None else 0

            if diff_level > 0:
                partes.append(f"+{diff_level} lvl")
            if diff_magic > 0:
                partes.append(f"+{diff_magic} magic")
            if diff_xp >= 30_000_000:
                partes.append(f"+{int(diff_xp / 1_000_000)}kk XP")
            if partes:
                extra = " (🆙 " + ", ".join(partes) + ")"

        msg += f"{medalha} _**{nome}** ➤ Level **{level}** | 🪄 Magic **{magic}**{extra}_\n"
        novo_rank[nome] = {"level": level, "magic": magic, "xp": xp}

    salvar_json(ARQUIVO_RANK, novo_rank)
    return msg


# =========================
# TOP LEVEL GLOBAL
# =========================
def top7_level():
    try:
        soup = soup_get(HIGHSCORE_XP)
        jogadores = []
        for row in soup.find_all("tr")[1:51]:
            cols = row.find_all("td")
            if len(cols) >= 4:
                nome = cols[1].text.strip().replace("Online", "").strip()
                jogadores.append({
                    "nome": nome,
                    "level": int(cols[2].text.strip()),
                    "xp": int(cols[3].text.strip().replace(",", "")),
                })
        return sorted(jogadores, key=lambda x: (x["level"], x["xp"]), reverse=True)[:7]
    except Exception as e:
        print(f"[tracker] erro rank level: {e}")
        return []


def gerar_msg_rank_level():
    top7 = top7_level()
    rank_antigo = carregar_json(ARQUIVO_RANK_LEVEL, {})
    msg = "🏆 **TOP 7 LEVEL GLOBAL** 🏆\n\n"

    if not top7:
        return msg + "_Erro ao carregar ranking_"

    nomes = [p["nome"] for p in top7]
    skills = pegar_skills_players(nomes)
    novo_rank = {}

    for i, player in enumerate(top7, 1):
        nome = player["nome"]
        level = player["level"]
        xp = player["xp"]
        skill = skills.get(nome, {"melee": 0, "distance": 0, "magic": 0})
        medalha = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣"][i - 1]
        extra = ""

        if nome in rank_antigo:
            antigo = rank_antigo[nome]
            partes = []
            diff_level = level - antigo.get("level", 0)
            diff_xp = xp - antigo.get("xp", 0)
            diff_melee = skill["melee"] - antigo.get("melee", 0)
            diff_dist = skill["distance"] - antigo.get("distance", 0)
            diff_magic = skill["magic"] - antigo.get("magic", 0)

            if diff_level > 0:
                partes.append(f"+{diff_level} lvl")
            if diff_xp >= 30_000_000:
                partes.append(f"+{int(diff_xp / 1_000_000)}kk XP")
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
            "magic": skill["magic"],
        }

    salvar_json(ARQUIVO_RANK_LEVEL, novo_rank)
    return msg


# =========================
# PEACE KILLERS
# =========================
def pegar_membros_hunted():
    r = session.get(HUNTED_URL, timeout=REQUEST_TIMEOUT)
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
        except Exception:
            continue
        membros.append(nome)
        levels[nome] = level

    return membros, levels


def analisar_hunted():
    membros, levels_atuais = pegar_membros_hunted()
    dados_antigos = carregar_json(ARQUIVO_HUNTED, {"membros": [], "levels": {}})
    membros_antigos = dados_antigos.get("membros", [])
    levels_antigos = dados_antigos.get("levels", {})
    data, hora = data_hora_brasil()
    momento = f"{data} • {hora}"

    entraram = []
    sairam = []
    if membros_antigos:
        for nome in membros:
            if nome not in membros_antigos:
                entraram.append((nome, levels_atuais.get(nome, 0), momento))
        for nome in membros_antigos:
            if nome not in membros:
                sairam.append((nome, levels_antigos.get(nome, 0), momento))

    ups = []
    downs = []
    for nome, level in levels_atuais.items():
        if nome in levels_antigos:
            antigo = levels_antigos[nome]
            diff = level - antigo
            if diff > 0:
                ups.append((nome, antigo, level, momento))
            elif diff < 0:
                # Downs ficam exclusivamente no canal mob xp peace.
                downs.append((nome, antigo, level, momento))

    total = len(levels_atuais)
    media = round(sum(levels_atuais.values()) / total) if total else 0
    l600 = sum(1 for l in levels_atuais.values() if l >= 600)
    l700 = sum(1 for l in levels_atuais.values() if l >= 700)
    l800 = sum(1 for l in levels_atuais.values() if l >= 800)

    salvar_json(ARQUIVO_HUNTED, {"membros": membros, "levels": levels_atuais})
    return total, media, l600, l700, l800, entraram, sairam, ups


def gerar_msg_hunted():
    total, media, l600, l700, l800, entraram, sairam, ups = analisar_hunted()
    data, hora = data_hora_brasil()
    msg = f"_🕒 Atualizado em: {data} • {hora}_\n\n"
    msg += "🎯 **RELATÓRIO — PEACE KILLERS (HUNTED)** 🎯\n\n"
    msg += f"👥 **Membros:** {total}\n"
    msg += f"⚔️ **Média de level:** {media}\n\n"
    msg += "📊 **Distribuição de levels**\n"
    msg += f"_Level 800+ ➤ {l800} membros_\n"
    msg += f"_Level 700+ ➤ {l700} membros_\n"
    msg += f"_Level 600+ ➤ {l600} membros_\n\n"

    msg += "📥 **Entraram**\n"
    msg += "\n".join(f"_**{n}** ➤ lvl {lvl} • {momento}_" for n, lvl, momento in entraram) if entraram else "_Nenhum_"

    msg += "\n\n📤 **Saíram**\n"
    msg += "\n".join(f"_**{n}** ➤ lvl {lvl} • {momento}_" for n, lvl, momento in sairam) if sairam else "_Nenhum_"

    msg += "\n\n📈 **Ups de level**\n"
    msg += "\n".join(f"_**{n}** ➤ {a} → {b} (+{b-a}) • {momento}_" for n, a, b, momento in ups) if ups else "_Nenhum_"

    return msg


# =========================
# FUNÇÕES PÚBLICAS
# =========================
def atualizar_spy_rank():
    """Envia uma NOVA mensagem no canal #spy-rank.

    Usado na primeira execução e depois todos os dias às 03:00.
    Não edita mensagem antiga.
    """
    msg = gerar_msg_rank_mage()
    msg += "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += gerar_msg_rank_level()
    enviar_em_partes("spy_rank", msg)


def atualizar_peace_killers():
    """Envia uma NOVA mensagem no canal #peace-killers.

    Usado na primeira execução e depois todos os dias às 03:00.
    Não edita mensagem antiga.
    """
    enviar_em_partes("peace_killers", gerar_msg_hunted())


# =========================
# MOB XP PEACE KILLERS
# =========================
def criar_eventos_mob_xp_peace(level_downs):
    """Cria eventos de level down da Peace Killers em ordem cronológica."""
    data, hora = data_hora_segundos_brasil()
    eventos = []

    for nome, antigo, novo in level_downs:
        eventos.append({
            "data": data,
            "hora": hora,
            "timestamp": f"{data} {hora}",
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "diff": antigo - novo,
        })

    return eventos


def carregar_historico_mob_xp_peace():
    historico = carregar_json(ARQUIVO_HISTORICO_MOB_XP_PEACE, [])
    return historico if isinstance(historico, list) else []


def salvar_historico_mob_xp_peace(historico):
    salvar_json(ARQUIVO_HISTORICO_MOB_XP_PEACE, historico)


def gerar_msg_mob_xp_peace_historico(historico):
    data, hora = data_hora_brasil()

    msg = "📉 **Histórico de Mob XP (Peace Killers):**\n\n"
    msg += f"📊 **Total de Mob XP registrados:** {len(historico)}\n\n"

    if historico:
        for e in historico:
            msg += (
                f"• `{e.get('timestamp')}` — **{e.get('nome')}** "
                f"(Lv {e.get('antigo')} → {e.get('novo')}) 🔻 -{e.get('diff')}\n"
            )
    else:
        msg += "_Nenhum mob XP registrado ainda._\n"

    msg += f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"
    return msg


def criar_novo_painel_mob_xp_peace(mensagem):
    """Cria uma nova mensagem ativa no #mob-xp-peace e salva o ID."""
    estado = carregar_json(ARQUIVO_ESTADO, {})
    novo_id = enviar("mob_xp_peace", mensagem)
    if novo_id:
        estado["mob_xp_peace"] = novo_id
        salvar_json(ARQUIVO_ESTADO, estado)
    return novo_id


def atualizar_painel_mob_xp_peace_com_rotacao(eventos):
    """Atualiza a mensagem fixa de Mob XP Peace.

    Se passar do limite do Discord, cria uma nova mensagem zerada e passa
    a editar essa nova mensagem. A antiga fica no canal como histórico.
    """
    historico_atual = carregar_historico_mob_xp_peace()
    historico_tentativo = historico_atual + eventos
    mensagem_tentativa = gerar_msg_mob_xp_peace_historico(historico_tentativo)

    if len(mensagem_tentativa) >= DISCORD_LIMITE:
        print("[tracker] histórico de #mob-xp-peace chegou perto do limite. criando nova mensagem zerada.")
        historico_novo = eventos
        mensagem_nova = gerar_msg_mob_xp_peace_historico(historico_novo)
        salvar_historico_mob_xp_peace(historico_novo)
        criar_novo_painel_mob_xp_peace(mensagem_nova)
        return

    salvar_historico_mob_xp_peace(historico_tentativo)
    atualizar_painel("mob_xp_peace", "mob_xp_peace", mensagem_tentativa)


def monitorar_mob_xp_peace():
    """Monitora level downs dos membros da Peace Killers a cada ciclo.

    Primeira execução apenas salva a base, sem mandar alerta falso.
    Depois, se algum membro descer level, adiciona no histórico e edita a
    mensagem ativa do canal #mob-xp-peace.
    """
    _, levels_atuais = pegar_membros_hunted()
    if not levels_atuais:
        print("[tracker] nenhum membro da Peace encontrado. Mob XP ignorado.")
        return

    levels_antigos = carregar_json(ARQUIVO_MOB_XP_PEACE_LEVELS, None)

    if not levels_antigos:
        print("[tracker] primeira execução do Mob XP Peace. salvando base sem alertas.")
        salvar_json(ARQUIVO_MOB_XP_PEACE_LEVELS, levels_atuais)
        return

    downs = []
    for nome, level in levels_atuais.items():
        if nome in levels_antigos:
            antigo = levels_antigos[nome]
            if level < antigo:
                downs.append((nome, antigo, level))

    if downs:
        eventos = criar_eventos_mob_xp_peace(downs)
        atualizar_painel_mob_xp_peace_com_rotacao(eventos)
    else:
        print("[tracker] sem Mob XP Peace detectado.")

    salvar_json(ARQUIVO_MOB_XP_PEACE_LEVELS, levels_atuais)
