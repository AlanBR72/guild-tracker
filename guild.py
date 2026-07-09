import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import (
    ARQUIVO_ESTADO,
    ARQUIVO_HISTORICO_LEVELS,
    ARQUIVO_LEVELS,
    ARQUIVO_MEMBROS,
    ARQUIVO_QUASE_LEVEL,
    BRASIL,
    CHARACTER_URL,
    DISCORD_LIMITE,
    GUILD_URL,
    INATIVO_AVISO,
    INATIVO_REMOCAO,
    LEVEL_IMPORTANTES,
    MARGEM_QUASE_LEVEL,
    REQUEST_TIMEOUT,
    TAGS_VALIDAS,
    THREADS,
    USER_AGENT,
)
from discord_utils import atualizar_painel, enviar, enviar_em_partes
from storage import carregar_json, salvar_json
from utils import agora_brasil, data_hora_brasil, data_hora_segundos_brasil, dias_para_tempo, formatar_k

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# =========================
# SCRAPING DA GUILDA
# =========================
def pegar_membros():
    r = session.get(GUILD_URL, timeout=REQUEST_TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")

    membros = []
    guild_datas = {}
    levels = {}

    tabela = soup.select_one("table")
    if not tabela:
        return membros, guild_datas, levels

    for row in tabela.select("tr")[1:]:
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

        join_text = cols[2].get_text(strip=True)
        try:
            join_date = datetime.strptime(join_text, "%b %d, %Y")
            join_date = BRASIL.localize(join_date)
        except Exception:
            continue

        membros.append({"nome": nome, "level": level})
        guild_datas[nome] = join_date
        levels[nome] = level

    return membros, guild_datas, levels


def last_online_requests(nome):
    try:
        url = CHARACTER_URL.format(nome.replace(" ", "%20"))
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        linhas = soup.select("table.character-table tr")

        for row in linhas:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            titulo = cols[0].get_text(strip=True).lower()
            if titulo != "last online":
                continue

            texto = cols[1].get_text(strip=True).lower()
            if "currently online" in texto:
                return None

            match = re.search(r"(\d+)", texto)
            if not match:
                return None

            numero = int(match.group(1))

            if "day" in texto:
                return numero
            if "week" in texto:
                return numero * 7
            if "month" in texto:
                return numero * 30
            if "year" in texto:
                return numero * 365

        return None
    except Exception as e:
        print(f"[guild] erro ao pegar last online de {nome}: {e}")
        return None


# =========================
# COMPARAÇÕES RÁPIDAS
# =========================
def normalizar_membros_antigos(membros_antigos):
    if not membros_antigos:
        return {}
    if isinstance(membros_antigos, list):
        return {nome: "?" for nome in membros_antigos}
    return membros_antigos


def detectar_entradas_saidas(levels_atuais):
    membros_atuais = {nome: level for nome, level in levels_atuais.items()}
    membros_antigos = normalizar_membros_antigos(carregar_json(ARQUIVO_MEMBROS, {}))

    primeira_execucao = not membros_antigos
    entraram = []
    sairam = []

    if not primeira_execucao:
        for nome, level in membros_atuais.items():
            if nome not in membros_antigos:
                entraram.append({"nome": nome, "level": level})

        for nome, level in membros_antigos.items():
            if nome not in membros_atuais:
                sairam.append({"nome": nome, "level": level})

    return primeira_execucao, membros_atuais, entraram, sairam


def detectar_level_changes(levels_atuais):
    levels_antigos = carregar_json(ARQUIVO_LEVELS, {})
    primeira_execucao = not levels_antigos
    level_ups = []
    level_downs = []

    if not primeira_execucao:
        for nome, level in levels_atuais.items():
            if nome not in levels_antigos:
                continue
            antigo = levels_antigos[nome]
            diff = level - antigo
            if diff > 0:
                level_ups.append((nome, antigo, level, diff))
            elif diff < 0:
                level_downs.append((nome, antigo, level, abs(diff)))

    return primeira_execucao, level_ups, level_downs


def detectar_quase_levels(levels_atuais):
    notificados = carregar_json(ARQUIVO_QUASE_LEVEL, {})
    novos = []
    novo_estado = dict(notificados)

    for nome, level in levels_atuais.items():
        for alvo in LEVEL_IMPORTANTES:
            faltam = alvo - level
            if 0 < faltam <= MARGEM_QUASE_LEVEL:
                chave = f"{nome}|{alvo}|{level}"
                if notificados.get(chave):
                    continue
                novos.append((nome, level, alvo, faltam))
                novo_estado[chave] = True

    # Limpa registros antigos de jogadores que já passaram do alvo ou saíram da faixa.
    chaves_validas = set(novo_estado.keys())
    for chave in list(chaves_validas):
        try:
            nome, alvo_txt, level_txt = chave.split("|")
            alvo = int(alvo_txt)
            level = int(level_txt)
            level_atual = levels_atuais.get(nome)
            if level_atual is None or level_atual >= alvo or level_atual < alvo - MARGEM_QUASE_LEVEL:
                novo_estado.pop(chave, None)
        except Exception:
            novo_estado.pop(chave, None)

    return novos, novo_estado


# =========================
# ANÁLISE COMPLETA / VISÃO GERAL
# =========================
def calcular_inativos(membros):
    in20 = []
    in10 = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {
            executor.submit(last_online_requests, membro["nome"]): membro["nome"]
            for membro in membros
        }

        for future in as_completed(futures):
            nome = futures[future]
            dias = future.result()
            if dias is None:
                continue
            if dias >= INATIVO_REMOCAO:
                in20.append((nome, dias))
            elif dias >= INATIVO_AVISO:
                in10.append((nome, dias))

    return in20, in10


def calcular_status(membros, guild_datas, levels_atuais):
    distribuicao = {"800+": 0, "700-799": 0, "600-699": 0, "500-599": 0}

    for level in levels_atuais.values():
        if level >= 800:
            distribuicao["800+"] += 1
        elif level >= 700:
            distribuicao["700-799"] += 1
        elif level >= 600:
            distribuicao["600-699"] += 1
        elif level >= 500:
            distribuicao["500-599"] += 1

    top_levels = sorted(levels_atuais.items(), key=lambda x: x[1], reverse=True)[:5]
    forca_guilda = sum(levels_atuais.values())
    total_membros = len(membros)
    media_level = round(forca_guilda / total_membros) if total_membros else 0

    in20, in10 = calcular_inativos(membros)

    antigos = sorted(guild_datas.items(), key=lambda x: x[1])[:5]
    hoje = agora_brasil()
    membros_sem_tag = []

    for nome, join_date in guild_datas.items():
        dias_na_guilda = (hoje - join_date).days
        nome_lower = nome.lower()
        if dias_na_guilda > 20 and not any(tag in nome_lower for tag in TAGS_VALIDAS):
            membros_sem_tag.append((nome, dias_na_guilda, join_date))

    return {
        "distribuicao": distribuicao,
        "top_levels": top_levels,
        "forca_guilda": forca_guilda,
        "total_membros": total_membros,
        "media_level": media_level,
        "in20": in20,
        "in10": in10,
        "antigos": antigos,
        "membros_sem_tag": membros_sem_tag,
    }


# =========================
# MENSAGENS
# =========================
def gerar_msg_entrada_saida(entraram, sairam):
    data, hora = data_hora_brasil()
    msg = f"_🕒 Detectado em: {data} • {hora} (Brasil)_\n\n"
    msg += "📥📤 **ENTRADAS E SAÍDAS — GUILD**\n\n"

    msg += "📥 **Entraram na guilda**\n"
    if entraram:
        for p in sorted(entraram, key=lambda x: x["level"] if isinstance(x["level"], int) else 0, reverse=True):
            msg += f"_Lv {p['level']}_ **{p['nome']}**\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n📤 **Saíram da guilda**\n"
    if sairam:
        for p in sorted(sairam, key=lambda x: x["level"] if isinstance(x["level"], int) else 0, reverse=True):
            msg += f"_Lv {p['level']}_ **{p['nome']}**\n"
    else:
        msg += "_Nenhum_\n"

    return msg


def criar_eventos_levels(level_ups, level_downs, quase_levels):
    """Transforma mudanças de level em eventos de histórico.

    A primeira execução não chama esta função, então não gera alertas falsos.
    """
    data, hora, completo = data_hora_segundos_brasil()
    eventos = []

    for nome, antigo, novo, diff in level_ups:
        eventos.append({
            "tipo": "up",
            "data": data,
            "hora": completo,
            "timestamp": f"{data} {completo}",
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "diff": diff,
        })

    for nome, antigo, novo, diff in level_downs:
        eventos.append({
            "tipo": "down",
            "data": data,
            "hora": completo,
            "timestamp": f"{data} {completo}",
            "nome": nome,
            "antigo": antigo,
            "novo": novo,
            "diff": diff,
        })

    for nome, level, alvo, faltam in quase_levels:
        eventos.append({
            "tipo": "quase",
            "data": data,
            "hora": completo,
            "timestamp": f"{data} {completo}",
            "nome": nome,
            "level": level,
            "alvo": alvo,
            "faltam": faltam,
        })

    return eventos


def carregar_historico_levels():
    historico = carregar_json(ARQUIVO_HISTORICO_LEVELS, [])
    return historico if isinstance(historico, list) else []


def salvar_historico_levels(historico):
    # Mantém o histórico em ordem cronológica: mais antigos em cima, mais recentes embaixo.
    salvar_json(ARQUIVO_HISTORICO_LEVELS, historico)


def criar_novo_painel_up_levels(mensagem):
    """Cria uma nova mensagem ativa no #up-levels e salva o novo ID.

    Usado quando a mensagem atual chega perto do limite do Discord.
    A mensagem antiga fica no canal como histórico, e a nova começa um histórico limpo.
    """
    estado = carregar_json(ARQUIVO_ESTADO, {})
    novo_id = enviar("up_levels", mensagem)
    if novo_id:
        estado["up_levels"] = novo_id
        salvar_json(ARQUIVO_ESTADO, estado)
    return novo_id


def atualizar_painel_up_levels_com_rotacao(eventos):
    """Atualiza a mensagem fixa de #up-levels.

    Se o histórico passar do limite de caracteres do Discord, cria uma nova
    mensagem e zera o histórico antigo, mantendo somente os eventos novos
    nessa nova mensagem.
    """
    historico_atual = carregar_historico_levels()
    historico_tentativo = historico_atual + eventos
    mensagem_tentativa = gerar_msg_up_levels_historico(historico_tentativo)

    if len(mensagem_tentativa) >= DISCORD_LIMITE:
        print("[guild] histórico de #up-levels chegou perto do limite. criando nova mensagem zerada.")
        historico_novo = eventos
        mensagem_nova = gerar_msg_up_levels_historico(historico_novo)
        salvar_historico_levels(historico_novo)
        criar_novo_painel_up_levels(mensagem_nova)
        return

    salvar_historico_levels(historico_tentativo)
    atualizar_painel("up_levels", "up_levels", mensagem_tentativa)


def gerar_msg_up_levels_historico(historico):
    data, hora = data_hora_brasil()

    msg = "📈 **Histórico de Levels (Guilt Of Virtue):**\n\n"

    eventos_level = [e for e in historico if e.get("tipo") in ("up", "down")]
    eventos_quase = [e for e in historico if e.get("tipo") == "quase"]

    if eventos_level:
        for e in eventos_level:
            if e.get("tipo") == "up":
                msg += (
                    f"• `{e.get('timestamp')}` — **{e.get('nome')}** "
                    f"(Lv {e.get('antigo')} → {e.get('novo')}) 🆙 +{e.get('diff')}\n"
                )
            elif e.get("tipo") == "down":
                msg += (
                    f"• `{e.get('timestamp')}` — **{e.get('nome')}** "
                    f"(Lv {e.get('antigo')} → {e.get('novo')}) 🔻 -{e.get('diff')}\n"
                )
    else:
        msg += "_Nenhum level up/down registrado ainda._\n"

    if eventos_quase:
        msg += "\n🎯 **Quase level importante:**\n"
        for e in eventos_quase:
            faltam_txt = "falta" if e.get("faltam") == 1 else "faltam"
            msg += (
                f"• `{e.get('timestamp')}` — **{e.get('nome')}** — "
                f"Lv **{e.get('level')}** ({faltam_txt} **{e.get('faltam')}** para {e.get('alvo')})\n"
            )

    msg += f"\n_🕒 Atualizado em: {data} • {hora} (Brasil)_"
    return msg


def gerar_msg_up_levels(level_ups, level_downs, quase_levels):
    """Compatibilidade: gera uma mensagem no formato antigo somente se algum módulo ainda chamar."""
    eventos = criar_eventos_levels(level_ups, level_downs, quase_levels)
    return gerar_msg_up_levels_historico(eventos)


def gerar_msg_visao_geral(status):
    data, hora = data_hora_brasil()
    msg = f"_🕒 Atualizado em: {data} • {hora} (Brasil)_\n\n"
    msg += "**🏆 ═══════ ESTATÍSTICAS DA GUILDA ═══════ 🏆**\n\n"
    msg += f"👥 **Membros:** {status['total_membros']}\n"
    msg += f"💪 **Força da Guilda:** _{formatar_k(status['forca_guilda'])}_\n"
    msg += f"⚔️ **Média de level da guilda:** _{status['media_level']}_\n\n"

    msg += "🏆 **Top 5 maiores levels da guilda**\n"
    for pos, (nome, level) in enumerate(status["top_levels"], start=1):
        medalha = ["🔥", "🥈", "🥉", "4️⃣", "5️⃣"][pos - 1]
        msg += f"{medalha} _{nome} ➤ level {level}_\n"

    msg += "\n👴 **5 Membros mais antigos da guilda:**\n"
    for pos, (nome, data_entrada) in enumerate(status["antigos"], start=1):
        dias = (agora_brasil() - data_entrada).days
        medalha = ["🥇", "🥈", "🥉", "🎖️", "🏅"][pos - 1]
        msg += f"{medalha} _{nome} ➤ {dias_para_tempo(dias)}_\n"

    msg += "\n📊 **Distribuição de levels**\n"
    dist = status["distribuicao"]
    msg += f"_Level 800+ ➤ {dist['800+']} membros_\n"
    msg += f"_Level 700-799 ➤ {dist['700-799']} membros_\n"
    msg += f"_Level 600-699 ➤ {dist['600-699']} membros_\n"
    msg += f"_Level 500-599 ➤ {dist['500-599']} membros_\n"

    msg += "\n**🚫 ═══════ MEMBROS INATIVOS ═══════ 🚫**\n\n"
    msg += "🚫 **Inativos há mais de 20 dias**\n"
    if status["in20"]:
        for nome, dias in sorted(status["in20"], key=lambda x: x[1], reverse=True):
            dias_txt = "30+ dias" if dias >= 30 else f"{dias} dias"
            msg += f"_{nome} ➤ {dias_txt}_\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n⚠️ **Inativos há mais de 10 dias**\n"
    if status["in10"]:
        for nome, dias in sorted(status["in10"], key=lambda x: x[1], reverse=True):
            msg += f"_{nome} ➤ {dias} dias_\n"
    else:
        msg += "_Nenhum_\n"

    msg += "\n**🏷️ ═══════ MEMBROS SEM TAG ═══════ 🏷️**\n\n"
    msg += "❌ **Membros há mais de 20 dias sem tag (Virtue / Culpa / Pravus):**\n"
    if status["membros_sem_tag"]:
        for nome, dias, _ in sorted(status["membros_sem_tag"], key=lambda x: x[1], reverse=True):
            msg += f"_{nome} ➤ {dias_para_tempo(dias)}_\n"
    else:
        msg += "_Nenhum_\n"

    return msg


# =========================
# FUNÇÕES PÚBLICAS
# =========================
def monitorar_guilda():
    membros, _, levels_atuais = pegar_membros()
    if not levels_atuais:
        print("[guild] nenhum membro encontrado. monitoramento ignorado.")
        return

    primeira_membros, membros_atuais, entraram, sairam = detectar_entradas_saidas(levels_atuais)
    primeira_levels, level_ups, level_downs = detectar_level_changes(levels_atuais)
    quase_levels, novo_estado_quase = detectar_quase_levels(levels_atuais)

    # Primeira execução apenas cria base, sem mandar alerta falso.
    if primeira_membros or primeira_levels:
        print("[guild] primeira execução detectada. salvando base sem enviar alertas.")
        salvar_json(ARQUIVO_MEMBROS, membros_atuais)
        salvar_json(ARQUIVO_LEVELS, levels_atuais)
        salvar_json(ARQUIVO_QUASE_LEVEL, novo_estado_quase)
        return

    if entraram or sairam:
        enviar_em_partes("entrada_saida", gerar_msg_entrada_saida(entraram, sairam))

    if level_ups or level_downs or quase_levels:
        eventos = criar_eventos_levels(level_ups, level_downs, quase_levels)
        atualizar_painel_up_levels_com_rotacao(eventos)

    salvar_json(ARQUIVO_MEMBROS, membros_atuais)
    salvar_json(ARQUIVO_LEVELS, levels_atuais)
    salvar_json(ARQUIVO_QUASE_LEVEL, novo_estado_quase)

    if not (entraram or sairam or level_ups or level_downs or quase_levels):
        print("[guild] sem mudanças detectadas.")


def atualizar_visao_geral():
    membros, guild_datas, levels_atuais = pegar_membros()
    if not levels_atuais:
        print("[guild] nenhum membro encontrado. visão geral não atualizada.")
        return
    status = calcular_status(membros, guild_datas, levels_atuais)
    msg = gerar_msg_visao_geral(status)
    atualizar_painel("visao_geral", "visao_geral", msg)
