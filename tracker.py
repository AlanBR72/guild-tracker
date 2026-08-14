import os
import re
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from config import (
    ARQUIVO_RANK,
    ARQUIVO_RANK_LEVEL,
    HIGHSCORE_DISTANCE,
    HIGHSCORE_MAGIC,
    HIGHSCORE_MELEE,
    HIGHSCORE_XP,
    BRASIL,
    ARQUIVO_ESTADO,
    DISCORD_LIMITE,
    GUILDAS_HUNTED,
    HUNTED_DATA_FOLDER,
    url_guilda_hunted,
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
# GUILDAS HUNTED
# =========================
def slug_guilda(nome: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", nome.lower()).strip("_")
    return slug or "guilda"


def caminho_hunted(nome: str, tipo: str) -> str:
    return f"{HUNTED_DATA_FOLDER}/{slug_guilda(nome)}_{tipo}.json"


def chave_estado_hunted(tipo: str, nome: str) -> str:
    return f"hunted_{tipo}_{slug_guilda(nome)}"


def garantir_hunted_folder():
    os.makedirs(HUNTED_DATA_FOLDER, exist_ok=True)


def pegar_membros_guilda_hunted(nome_guilda: str, cfg: dict):
    """Retorna membros, levels e datas de entrada de uma guilda hunted."""
    url = url_guilda_hunted(nome_guilda, cfg)
    try:
        soup = soup_get(url)
    except Exception as e:
        print(f"[tracker] erro ao abrir {nome_guilda}: {e}")
        return [], {}, {}

    membros = []
    levels = {}
    guild_datas = {}
    tabela = soup.select_one("table")
    if not tabela:
        print(f"[tracker] tabela não encontrada em {nome_guilda}")
        return membros, levels, guild_datas

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

        join_iso = None
        try:
            join_text = cols[2].get_text(strip=True)
            join_date = datetime.strptime(join_text, "%b %d, %Y")
            join_iso = BRASIL.localize(join_date).isoformat()
        except Exception:
            pass

        membros.append(nome)
        levels[nome] = level
        guild_datas[nome] = join_iso

    return membros, levels, guild_datas


def normalizar_membros_hunted(dados):
    if not dados:
        return {}
    if isinstance(dados, list):
        return {nome: {"level": "?", "join": None} for nome in dados}

    normalizado = {}
    if isinstance(dados, dict):
        for nome, valor in dados.items():
            if isinstance(valor, dict):
                normalizado[nome] = {
                    "level": valor.get("level", "?"),
                    "join": valor.get("join"),
                }
            else:
                normalizado[nome] = {"level": valor, "join": None}
    return normalizado


def separar_trocas_nick_hunted(entraram, sairam):
    """Pareia troca de nick somente dentro da mesma guilda."""
    trocas = []
    entradas_restantes = list(entraram)
    saidas_restantes = []

    for saiu in sairam:
        candidatos = []
        for entrou in entradas_restantes:
            if entrou.get("level") != saiu.get("level"):
                continue

            join_saiu = saiu.get("join")
            join_entrou = entrou.get("join")
            if join_saiu and join_entrou:
                if join_saiu == join_entrou:
                    candidatos.append(entrou)
            elif not join_saiu or not join_entrou:
                candidatos.append(entrou)

        if len(candidatos) == 1:
            entrou = candidatos[0]
            trocas.append({
                "antigo": saiu.get("nome"),
                "novo": entrou.get("nome"),
                "level": entrou.get("level"),
            })
            entradas_restantes.remove(entrou)
        else:
            saidas_restantes.append(saiu)

    return entradas_restantes, saidas_restantes, trocas


def criar_eventos_movimentacao(entraram, sairam, trocas):
    data, hora = data_hora_brasil()
    timestamp = f"{data} {hora}"
    eventos = []

    for p in entraram:
        eventos.append({
            "tipo": "entrada",
            "timestamp": timestamp,
            "nome": p.get("nome"),
            "level": p.get("level"),
        })
    for p in sairam:
        eventos.append({
            "tipo": "saida",
            "timestamp": timestamp,
            "nome": p.get("nome"),
            "level": p.get("level"),
        })
    for p in trocas:
        eventos.append({
            "tipo": "nick",
            "timestamp": timestamp,
            "antigo": p.get("antigo"),
            "novo": p.get("novo"),
            "level": p.get("level"),
        })
    return eventos


def gerar_msg_movimentacoes_hunted(nome_guilda: str, historico: list) -> str:
    data, hora = data_hora_brasil()
    msg = f"📋 **Histórico de Entradas e Saídas ({nome_guilda}):**\n\n"

    for evento in historico:
        tipo = evento.get("tipo")
        if tipo == "entrada":
            msg += (
                f"• `{evento.get('timestamp')}` — 🟢 **{evento.get('nome')}** "
                f"entrou na guilda (Lv {evento.get('level')})\n"
            )
        elif tipo == "saida":
            msg += (
                f"• `{evento.get('timestamp')}` — 🔴 **{evento.get('nome')}** "
                f"saiu da guilda (Lv {evento.get('level')})\n"
            )
        elif tipo == "nick":
            msg += (
                f"• `{evento.get('timestamp')}` — 🔁 **{evento.get('antigo')}** "
                f"alterou o nick para **{evento.get('novo')}** "
                f"(Lv {evento.get('level')})\n"
            )

    msg += f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"
    return msg


def criar_eventos_mob_xp(level_downs):
    data, hora = data_hora_brasil()
    timestamp = f"{data} {hora}"
    return [
        {
            "timestamp": timestamp,
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "diff": antigo - novo,
        }
        for nome, antigo, novo in level_downs
    ]


def gerar_msg_mob_xp_hunted(nome_guilda: str, historico: list) -> str:
    data, hora = data_hora_brasil()
    msg = f"📉 **Histórico de Mob XP ({nome_guilda}):**\n\n"
    msg += f"📊 **Total de Mob XP registrados nesta lista:** {len(historico)}\n\n"

    for evento in historico:
        msg += (
            f"• `{evento.get('timestamp')}` — **{evento.get('nome')}** "
            f"(Lv {evento.get('antigo')} → {evento.get('novo')}) "
            f"🔻 -{evento.get('diff')}\n"
        )

    msg += f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"
    return msg


def atualizar_historico_separado(
    nome_guilda: str,
    tipo: str,
    canal: str,
    eventos: list,
    gerador_mensagem,
):
    """Mantém uma mensagem ativa separada para cada guilda.

    Ao alcançar o limite, deixa a mensagem antiga intacta, envia um aviso e
    cria uma nova lista contendo somente os eventos novos.
    """
    if not eventos:
        return

    garantir_hunted_folder()
    arquivo_historico = caminho_hunted(nome_guilda, f"historico_{tipo}")
    historico_atual = carregar_json(arquivo_historico, [])
    if not isinstance(historico_atual, list):
        historico_atual = []

    tentativa = historico_atual + eventos
    mensagem_tentativa = gerador_mensagem(nome_guilda, tentativa)
    estado = carregar_json(ARQUIVO_ESTADO, {})
    estado_key = chave_estado_hunted(tipo, nome_guilda)

    if len(mensagem_tentativa) >= DISCORD_LIMITE:
        aviso = (
            f"⚠️ **O histórico de {nome_guilda} chegou ao limite de caracteres.**\n"
            "Uma nova lista será iniciada abaixo."
        )
        enviar(canal, aviso)

        novo_historico = list(eventos)
        nova_mensagem = gerador_mensagem(nome_guilda, novo_historico)
        novo_id = enviar(canal, nova_mensagem)
        if novo_id:
            estado[estado_key] = novo_id
            salvar_json(ARQUIVO_ESTADO, estado)
            salvar_json(arquivo_historico, novo_historico)
        return

    msg_id = estado.get(estado_key)
    novo_id = editar(canal, msg_id, mensagem_tentativa)
    if novo_id:
        estado[estado_key] = novo_id
        salvar_json(ARQUIVO_ESTADO, estado)
        salvar_json(arquivo_historico, tentativa)


def ciclo_diario_hunted() -> str:
    """Retorna a data do ciclo diário, que começa às 03:00 no Brasil."""
    agora = datetime.now(BRASIL)
    inicio_hoje = agora.replace(hour=3, minute=0, second=0, microsecond=0)
    data_ciclo = agora.date() if agora >= inicio_hoje else (agora - timedelta(days=1)).date()
    return data_ciclo.isoformat()


def carregar_estado_spy_info_guilda(nome_guilda: str) -> dict:
    arquivo = caminho_hunted(nome_guilda, "spy_info_diario")
    estado = carregar_json(arquivo, {})
    if not isinstance(estado, dict):
        estado = {}
    ciclo = ciclo_diario_hunted()
    if estado.get("ciclo") != ciclo:
        estado = {"ciclo": ciclo, "ups": []}
        salvar_json(arquivo, estado)
    if not isinstance(estado.get("ups"), list):
        estado["ups"] = []
    return estado


def salvar_estado_spy_info_guilda(nome_guilda: str, estado: dict) -> None:
    salvar_json(caminho_hunted(nome_guilda, "spy_info_diario"), estado)


def acumular_ups_spy_info(nome_guilda: str, ups: list) -> None:
    """Acumula ups sem duplicar dentro do ciclo diário atual."""
    if not ups:
        return
    estado = carregar_estado_spy_info_guilda(nome_guilda)
    existentes = {
        (str(e.get("nome")), int(e.get("antigo", 0)), int(e.get("novo", 0)))
        for e in estado.get("ups", [])
        if isinstance(e, dict)
    }
    data, hora = data_hora_brasil()
    for nome, antigo, novo in ups:
        chave = (str(nome), int(antigo), int(novo))
        if chave in existentes:
            continue
        estado["ups"].append({
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "timestamp": f"{data} {hora}",
        })
        existentes.add(chave)
    salvar_estado_spy_info_guilda(nome_guilda, estado)


def monitorar_uma_guilda_hunted(nome_guilda: str, cfg: dict) -> bool:
    """Monitora uma guilda hunted e retorna True se houve mudança relevante."""
    membros, levels_atuais, guild_datas = pegar_membros_guilda_hunted(nome_guilda, cfg)
    if not levels_atuais:
        print(f"[tracker] nenhum membro encontrado em {nome_guilda}; ciclo ignorado.")
        return False

    houve_mudanca = False

    garantir_hunted_folder()

    # -------------------------
    # Entradas, saídas e nicks
    # -------------------------
    atuais = {
        nome: {"level": levels_atuais[nome], "join": guild_datas.get(nome)}
        for nome in membros
    }
    arquivo_membros = caminho_hunted(nome_guilda, "membros")
    antigos = normalizar_membros_hunted(carregar_json(arquivo_membros, {}))

    if not antigos:
        print(f"[tracker] primeira base de membros salva para {nome_guilda}.")
        salvar_json(arquivo_membros, atuais)
    else:
        entraram = [
            {"nome": nome, "level": dados.get("level"), "join": dados.get("join")}
            for nome, dados in atuais.items() if nome not in antigos
        ]
        sairam = [
            {"nome": nome, "level": dados.get("level"), "join": dados.get("join")}
            for nome, dados in antigos.items() if nome not in atuais
        ]

        if entraram or sairam:
            houve_mudanca = True
            entradas_restantes, saidas_restantes, trocas = separar_trocas_nick_hunted(
                entraram, sairam
            )
            eventos = criar_eventos_movimentacao(
                entradas_restantes, saidas_restantes, trocas
            )
            atualizar_historico_separado(
                nome_guilda,
                "movimentacoes",
                "saida_membros",
                eventos,
                gerar_msg_movimentacoes_hunted,
            )
        else:
            print(f"[tracker] sem movimentações em {nome_guilda}.")

        salvar_json(arquivo_membros, atuais)

    # -------------------------
    # Mob XP / level downs
    # -------------------------
    arquivo_levels = caminho_hunted(nome_guilda, "levels_mob_xp")
    levels_antigos = carregar_json(arquivo_levels, None)

    if not levels_antigos:
        print(f"[tracker] primeira base de Mob XP salva para {nome_guilda}.")
        salvar_json(arquivo_levels, levels_atuais)
        return houve_mudanca

    # Qualquer alteração de level é relevante para o #spy-info.
    if levels_atuais != levels_antigos:
        houve_mudanca = True

    downs = []
    ups = []
    for nome, level in levels_atuais.items():
        if nome not in levels_antigos:
            continue
        if level < levels_antigos[nome]:
            downs.append((nome, levels_antigos[nome], level))
        elif level > levels_antigos[nome]:
            ups.append((nome, levels_antigos[nome], level))

    acumular_ups_spy_info(nome_guilda, ups)

    if downs:
        atualizar_historico_separado(
            nome_guilda,
            "mob_xp",
            "mob_xp",
            criar_eventos_mob_xp(downs),
            gerar_msg_mob_xp_hunted,
        )
    else:
        print(f"[tracker] sem Mob XP em {nome_guilda}.")

    salvar_json(arquivo_levels, levels_atuais)
    return houve_mudanca


def monitorar_guildas_hunted() -> bool:
    """Monitora todas as hunted e retorna True se alguma mudou."""
    houve_mudanca_geral = False
    for nome_guilda, cfg in GUILDAS_HUNTED.items():
        try:
            if monitorar_uma_guilda_hunted(nome_guilda, cfg):
                houve_mudanca_geral = True
        except Exception as e:
            print(f"[tracker] erro ao monitorar {nome_guilda}: {e}")
    return houve_mudanca_geral


def gerar_msg_spy_info_guilda(nome_guilda: str, cfg: dict) -> str:
    """Gera o painel diário independente de uma guilda hunted."""
    garantir_hunted_folder()
    _, levels_atuais, _ = pegar_membros_guilda_hunted(nome_guilda, cfg)
    data, hora = data_hora_brasil()

    msg = f"🎯 **{nome_guilda.upper()}** (HUNTED)\n\n"
    msg += f"_🕒 Atualizado em: {data} • {hora}_\n\n"

    if not levels_atuais:
        return msg + "_Erro ao carregar a guilda._"

    total = len(levels_atuais)
    media = round(sum(levels_atuais.values()) / total) if total else 0
    l600 = sum(1 for level in levels_atuais.values() if level >= 600)
    l700 = sum(1 for level in levels_atuais.values() if level >= 700)
    l800 = sum(1 for level in levels_atuais.values() if level >= 800)

    estado_diario = carregar_estado_spy_info_guilda(nome_guilda)
    ups = estado_diario.get("ups", [])

    msg += f"👥 Membros: `{total}`\n"
    msg += f"⚔️ Média: `Lv.{media}`\n\n"
    msg += "📊 **Distribuição**\n"
    msg += f"🔸 `800+` ➜ `{l800}`\n"
    msg += f"🔸 `700+` ➜ `{l700}`\n"
    msg += f"🔸 `600+` ➜ `{l600}`\n\n"
    msg += "📈 **Últimos ups**\n"

    if not ups:
        return msg + "_Nenhum_"

    linhas = []
    for evento in ups:
        timestamp = str(evento.get("timestamp", ""))
        # O timestamp é salvo como DD/MM/YY HH:MM; no painel mostramos somente HH:MM.
        hora_up = timestamp.split(" ", 1)[1] if " " in timestamp else timestamp
        linhas.append(
            f"• **{evento.get('nome')}** `{evento.get('antigo')} → {evento.get('novo')}` • `{hora_up}`"
        )

    # Proteção final contra o limite do Discord. Mantém os ups mais recentes.
    omitidos = 0
    while linhas:
        sufixo = f"\n_... {omitidos} up(s) mais antigo(s) omitido(s) pelo limite._" if omitidos else ""
        tentativa = msg + "\n".join(linhas) + sufixo
        if len(tentativa) < DISCORD_LIMITE:
            return tentativa
        linhas.pop(0)
        omitidos += 1

    return msg + "_Nenhum_"


def atualizar_spy_info(novo_ciclo: bool = False):
    """Mantém um painel diário separado para cada guilda hunted.

    A troca real de ciclo é controlada por uma chave global persistida. Assim,
    mesmo se o bot reiniciar entre 03:00 e 03:10, não cria painéis duplicados.
    """
    try:
        garantir_hunted_folder()
        estado_geral = carregar_json(ARQUIVO_ESTADO, {})
        ciclo_atual = ciclo_diario_hunted()
        ciclo_ativo = estado_geral.get("hunted_spy_info_ciclo_ativo")
        precisa_novo_ciclo = ciclo_ativo != ciclo_atual

        # novo_ciclo sinaliza a rotina das 03:00, mas a chave persistida impede
        # duplicação caso a rotina rode novamente no mesmo ciclo.
        if novo_ciclo and ciclo_ativo != ciclo_atual:
            precisa_novo_ciclo = True

        for nome_guilda, cfg in GUILDAS_HUNTED.items():
            if precisa_novo_ciclo:
                salvar_estado_spy_info_guilda(
                    nome_guilda, {"ciclo": ciclo_atual, "ups": []}
                )

            mensagem = gerar_msg_spy_info_guilda(nome_guilda, cfg)
            chave_id = chave_estado_hunted("spy_info", nome_guilda)
            msg_id = estado_geral.get(chave_id)

            if precisa_novo_ciclo or not msg_id:
                novo_id = enviar("spy_info", mensagem)
            else:
                novo_id = editar("spy_info", msg_id, mensagem)

            if novo_id:
                estado_geral[chave_id] = novo_id

        estado_geral["hunted_spy_info_ciclo_ativo"] = ciclo_atual
        salvar_json(ARQUIVO_ESTADO, estado_geral)
    except Exception as e:
        print(f"[tracker] erro ao atualizar #spy-info: {e}")


# =========================
# FUNÇÕES PÚBLICAS
# =========================
def atualizar_spy_rank():
    """Envia uma nova mensagem com os rankings globais."""
    msg = gerar_msg_rank_mage()
    msg += "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += gerar_msg_rank_level()
    enviar_em_partes("spy_rank", msg)


