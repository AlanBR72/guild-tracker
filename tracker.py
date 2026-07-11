import requests
from bs4 import BeautifulSoup

from config import (
    ARQUIVO_RANK,
    ARQUIVO_RANK_LEVEL,
    HIGHSCORE_DISTANCE,
    HIGHSCORE_MAGIC,
    HIGHSCORE_MELEE,
    HIGHSCORE_XP,
    GUILDAS_HUNTED,
    ARQUIVO_ESTADO,
    DATA_FOLDER,
    DISCORD_LIMITE,
    REQUEST_TIMEOUT,
    USER_AGENT,
)
from discord_utils import atualizar_painel, editar, enviar, enviar_em_partes
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
# GUILDAS HUNTED (CONFIGURÁVEIS)
# =========================
def caminho_hunted(chave: str, sufixo: str) -> str:
    return f"{DATA_FOLDER}/hunted_{chave}_{sufixo}.json"


def pegar_membros_hunted(config_guilda):
    """Retorna membros, levels e data de entrada da guilda configurada."""
    try:
        soup = soup_get(config_guilda["url"])
    except Exception as e:
        print(f"[tracker] erro ao abrir {config_guilda['nome']}: {e}")
        return [], {}, {}

    membros = []
    levels = {}
    joins = {}
    tabela = soup.select_one("table")
    if not tabela:
        return membros, levels, joins

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
        join_text = cols[2].get_text(" ", strip=True)
        membros.append(nome)
        levels[nome] = level
        joins[nome] = join_text

    return membros, levels, joins


def detectar_trocas_nick(entraram, sairam):
    """Pareia troca de nick por mesmo level e mesma data de entrada.

    Se a data não estiver disponível, não força o pareamento para evitar
    confundir duas pessoas diferentes com o mesmo level.
    """
    trocas = []
    entradas_usadas = set()
    saidas_usadas = set()

    for i, saida in enumerate(sairam):
        candidatos = []
        for j, entrada in enumerate(entraram):
            if j in entradas_usadas:
                continue
            if saida[1] == entrada[1] and saida[2] and saida[2] == entrada[2]:
                candidatos.append(j)
        if len(candidatos) == 1:
            j = candidatos[0]
            entrada = entraram[j]
            trocas.append({
                "antigo": saida[0],
                "novo": entrada[0],
                "level": entrada[1],
            })
            saidas_usadas.add(i)
            entradas_usadas.add(j)

    entradas_restantes = [e for i, e in enumerate(entraram) if i not in entradas_usadas]
    saidas_restantes = [s for i, s in enumerate(sairam) if i not in saidas_usadas]
    return entradas_restantes, saidas_restantes, trocas


def analisar_guilda_hunted(chave, config_guilda, salvar_estado=True):
    membros, levels, joins = pegar_membros_hunted(config_guilda)
    if not levels:
        return None

    arquivo = caminho_hunted(chave, "estado")
    antigo = carregar_json(arquivo, None)
    primeira = not antigo
    antigo = antigo or {"membros": [], "levels": {}, "joins": {}}

    membros_antigos = antigo.get("membros", [])
    levels_antigos = antigo.get("levels", {})
    joins_antigos = antigo.get("joins", {})

    entraram = [
        (nome, levels.get(nome, 0), joins.get(nome, ""))
        for nome in membros if nome not in membros_antigos
    ]
    sairam = [
        (nome, levels_antigos.get(nome, 0), joins_antigos.get(nome, ""))
        for nome in membros_antigos if nome not in membros
    ]
    entraram, sairam, trocas = detectar_trocas_nick(entraram, sairam)

    ups = []
    downs = []
    for nome, level in levels.items():
        if nome not in levels_antigos:
            continue
        anterior = levels_antigos[nome]
        if level > anterior:
            ups.append((nome, anterior, level))
        elif level < anterior:
            downs.append((nome, anterior, level))

    total = len(levels)
    resultado = {
        "primeira": primeira,
        "membros": membros,
        "levels": levels,
        "joins": joins,
        "entraram": entraram,
        "sairam": sairam,
        "trocas": trocas,
        "ups": ups,
        "downs": downs,
        "total": total,
        "media": round(sum(levels.values()) / total) if total else 0,
        "l600": sum(1 for x in levels.values() if x >= 600),
        "l700": sum(1 for x in levels.values() if x >= 700),
        "l800": sum(1 for x in levels.values() if x >= 800),
    }

    if salvar_estado:
        salvar_json(arquivo, {"membros": membros, "levels": levels, "joins": joins})
    return resultado


# =========================
# SPY INFO DIÁRIO
# =========================
def gerar_msg_spy_info():
    data, hora = data_hora_brasil()
    blocos = ["🎯 **RELATÓRIO — GUILDAS HUNTED** 🎯"]

    for chave, cfg in GUILDAS_HUNTED.items():
        dados = analisar_guilda_hunted(chave, cfg, salvar_estado=False)
        if not dados:
            blocos.append(f"🔥 **{cfg['nome'].upper()}**\n_Erro ao carregar guilda._")
            continue
        bloco = [
            f"🔥 **{cfg['nome'].upper()}**",
            "",
            f"👥 **Membros:** {dados['total']}",
            f"⚔️ **Média de level:** {dados['media']}",
            "",
            "📊 **Distribuição de levels**",
            f"_Level 800+ ➤ {dados['l800']} membros_",
            f"_Level 700+ ➤ {dados['l700']} membros_",
            f"_Level 600+ ➤ {dados['l600']} membros_",
        ]
        blocos.append("\n".join(bloco))

    return "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n".join(blocos) + f"\n\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"


def atualizar_spy_info():
    enviar_em_partes("spy_info", gerar_msg_spy_info())


# =========================
# HISTÓRICOS SEPARADOS POR GUILDA
# =========================
def carregar_historico(chave, tipo):
    dado = carregar_json(caminho_hunted(chave, f"historico_{tipo}"), [])
    return dado if isinstance(dado, list) else []


def salvar_historico(chave, tipo, historico):
    salvar_json(caminho_hunted(chave, f"historico_{tipo}"), historico)


def timestamp_agora():
    data, hora = data_hora_brasil()
    return f"{data} {hora}"


def gerar_msg_mob_xp(nome_guilda, historico):
    data, hora = data_hora_brasil()
    msg = f"📉 **Histórico de Mob XP — {nome_guilda}:**\n\n"
    if historico:
        for e in historico:
            msg += (
                f"• `{e['timestamp']}` — **{e['nome']}** "
                f"(Lv {e['antigo']} → {e['novo']}) 🔻 -{e['diff']}\n"
            )
    else:
        msg += "_Nenhum mob XP registrado ainda._\n"
    return msg + f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"


def gerar_msg_movimentacoes(nome_guilda, historico):
    data, hora = data_hora_brasil()
    msg = f"📋 **Histórico de Entradas e Saídas — {nome_guilda}:**\n\n"
    if historico:
        for e in historico:
            if e["tipo"] == "entrada":
                msg += f"• `{e['timestamp']}` — 🟢 **{e['nome']}** entrou na guilda (Lv {e['level']})\n"
            elif e["tipo"] == "saida":
                msg += f"• `{e['timestamp']}` — 🔴 **{e['nome']}** saiu da guilda (Lv {e['level']})\n"
            else:
                msg += (
                    f"• `{e['timestamp']}` — 🔁 **{e['antigo']}** alterou o nick para "
                    f"**{e['novo']}** (Lv {e['level']})\n"
                )
    else:
        msg += "_Nenhuma movimentação registrada ainda._\n"
    return msg + f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"


def atualizar_historico_com_rotacao(canal, estado_key, chave, tipo, nome_guilda, eventos, gerador):
    historico = carregar_historico(chave, tipo)
    tentativo = historico + eventos
    mensagem = gerador(nome_guilda, tentativo)

    if len(mensagem) >= DISCORD_LIMITE:
        aviso = (
            f"⚠️ **O histórico de {nome_guilda} chegou ao limite de caracteres.**\n"
            "Uma nova lista será iniciada abaixo."
        )
        enviar(canal, aviso)
        novo_historico = eventos
        nova_msg = gerador(nome_guilda, novo_historico)
        novo_id = enviar(canal, nova_msg)
        if novo_id:
            estado = carregar_json(ARQUIVO_ESTADO, {})
            estado[estado_key] = novo_id
            salvar_json(ARQUIVO_ESTADO, estado)
            salvar_historico(chave, tipo, novo_historico)
        return

    salvar_historico(chave, tipo, tentativo)
    atualizar_painel(canal, estado_key, mensagem)


def criar_eventos_movimentacoes(dados):
    ts = timestamp_agora()
    eventos = []
    for nome, level, _join in dados["entraram"]:
        eventos.append({"tipo": "entrada", "timestamp": ts, "nome": nome, "level": level})
    for nome, level, _join in dados["sairam"]:
        eventos.append({"tipo": "saida", "timestamp": ts, "nome": nome, "level": level})
    for troca in dados["trocas"]:
        eventos.append({"tipo": "nick", "timestamp": ts, **troca})
    return eventos


def criar_eventos_mob_xp(dados):
    ts = timestamp_agora()
    return [
        {
            "timestamp": ts,
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "diff": antigo - novo,
        }
        for nome, antigo, novo in dados["downs"]
    ]


def monitorar_guildas_hunted():
    """Monitora todas as guildas em GUILDAS_HUNTED a cada 5 minutos.

    Cada guilda mantém mensagens e arquivos de histórico próprios nos canais
    compartilhados #mob-xp e #saída-membros.
    """
    for chave, cfg in GUILDAS_HUNTED.items():
        dados = analisar_guilda_hunted(chave, cfg, salvar_estado=True)
        if not dados:
            print(f"[tracker] {cfg['nome']}: nenhum membro encontrado; ciclo ignorado.")
            continue
        if dados["primeira"]:
            print(f"[tracker] {cfg['nome']}: primeira execução, base salva sem alertas.")
            continue

        eventos_mov = criar_eventos_movimentacoes(dados)
        if eventos_mov:
            atualizar_historico_com_rotacao(
                "saida_membros_hunted",
                f"saida_membros_hunted::{chave}",
                chave,
                "movimentacoes",
                cfg["nome"],
                eventos_mov,
                gerar_msg_movimentacoes,
            )
        else:
            print(f"[tracker] {cfg['nome']}: sem entrada, saída ou troca de nick.")

        eventos_mob = criar_eventos_mob_xp(dados)
        if eventos_mob:
            atualizar_historico_com_rotacao(
                "mob_xp",
                f"mob_xp::{chave}",
                chave,
                "mob_xp",
                cfg["nome"],
                eventos_mob,
                gerar_msg_mob_xp,
            )
        else:
            print(f"[tracker] {cfg['nome']}: sem level down.")


# Compatibilidade com nomes antigos usados em versões anteriores.
def atualizar_peace_killers():
    atualizar_spy_info()


def monitorar_mob_xp_peace():
    monitorar_guildas_hunted()
