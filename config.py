import pytz
from urllib.parse import quote

# =========================
# URLs DO RUCOY
# =========================
GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"

# Adicione ou remova guildas hunted somente aqui.
# A chave é o nome exibido no Discord. A URL pode ser informada manualmente;
# se for omitida, ela é montada automaticamente pelo nome da guilda.
GUILDAS_HUNTED = {
    "Peace Killers": {
        "url": "https://www.rucoyonline.com/guild/Peace%20Killers",
    },
    "Infernal Cruelty": {
        "url": "https://www.rucoyonline.com/guild/Infernal%20Cruelty",
    },
    "The Void": {
        "url": "https://www.rucoyonline.com/guild/The%20Void",
    },
    "The Chaos Insurgency": {
    "url": "https://www.rucoyonline.com/guild/The%20Chaos%20Insurgency",
    }, 
}


def url_guilda_hunted(nome: str, config: dict) -> str:
    return config.get("url") or f"https://www.rucoyonline.com/guild/{quote(nome)}"


HIGHSCORE_XP = "https://www.rucoyonline.com/highscores/experience/2016/1"
HIGHSCORE_MAGIC = "https://www.rucoyonline.com/highscores/magic/2016/1"
HIGHSCORE_MELEE = "https://www.rucoyonline.com/highscores/melee/2016/1"
HIGHSCORE_DISTANCE = "https://www.rucoyonline.com/highscores/distance/2016/1"
CHARACTER_URL = "https://www.rucoyonline.com/characters/{}"

# =========================
# WEBHOOKS DO DISCORD
# =========================
WEBHOOK_SPY_RANK = "https://discord.com/api/webhooks/1494393213409300531/iX8kJAHYJdxQBZCGAOzb0vwC6HquvcfO6EZ2mFThwJ7phDDQbBqELMXcFW5t01P1rKYZ"
WEBHOOK_VISAO_GERAL = "https://discord.com/api/webhooks/1524443607837442170/GZ1t2ayHAY-pdNLWyo2dqXYHTkPQYY8tWDqJHKTOnmQrCkwK0EbI4ckPTVeUS1SqUwP_"
WEBHOOK_ENTRADA_SAIDA = "https://discord.com/api/webhooks/1481362798326972448/aRQkId2Le1rzymVrtXQHRgxv2c6RU7GPMrCcg7R6sQ_FXfGQv6xeaJjrOtCXYArL57Up"
WEBHOOK_UP_LEVELS = "https://discord.com/api/webhooks/1524443815920799888/kUNlK2oBN8CCqusF9XX4OJRPALOH4ehgbB066kmVONMsv-sW9G2NwaaM_1wYnRv2hyhq"

# Os três webhooks abaixo continuam sendo os mesmos; apenas os canais foram renomeados.
WEBHOOK_SPY_INFO = "https://discord.com/api/webhooks/1524443881045757983/rA2XNrujiBnp7lHh54b3MAkTTMesRbuqP9nEO44qjy7WP0e0jt_E8h7Hsu7etw1qiwyc"
WEBHOOK_MOB_XP = "https://discord.com/api/webhooks/1524793800688406642/OucCMAmMXTG-Pr3uWi9UoyS1rCIXiTHMlL4ASGZ199yiQ_ED5H5YP7hJ9bmGPrOlyU3-"
WEBHOOK_SAIDA_MEMBROS = "https://discord.com/api/webhooks/1525161252974760046/pSK7N_O_t75H2lVOKHHsMte2CuHEVRApMFxAggHaUAkeZNPrhglNVjCmbbL5OqZQ1BL6"

# =========================
# ARQUIVOS
# =========================
DATA_FOLDER = "data"
ARQUIVO_ESTADO = f"{DATA_FOLDER}/estado_msg.json"
ARQUIVO_MEMBROS = f"{DATA_FOLDER}/membros_guilda.json"
ARQUIVO_LEVELS = f"{DATA_FOLDER}/levels_guilda.json"
ARQUIVO_RANK = f"{DATA_FOLDER}/rank_mage.json"
ARQUIVO_RANK_LEVEL = f"{DATA_FOLDER}/rank_level.json"
ARQUIVO_QUASE_LEVEL = f"{DATA_FOLDER}/quase_level_notificado.json"
ARQUIVO_HISTORICO_LEVELS = f"{DATA_FOLDER}/historico_levels.json"
ARQUIVO_HISTORICO_ENTRADA_SAIDA = f"{DATA_FOLDER}/historico_entrada_saida.json"
ARQUIVO_LAST_ONLINE_CACHE = f"{DATA_FOLDER}/last_online_cache.json"

# Estados das guildas hunted ficam em arquivos dinâmicos dentro de data/hunted/.
HUNTED_DATA_FOLDER = f"{DATA_FOLDER}/hunted"

# =========================
# TEMPO / EXECUÇÃO
# =========================
BRASIL = pytz.timezone("America/Sao_Paulo")
INTERVALO_GUILDA = 600  # 10 minutos: entradas/saídas e levels da Virtue
INTERVALO_VISAO_GERAL = 1800  # 30 minutos: painel #visao-geral
INTERVALO_HUNTED = 300  # 5 minutos
INTERVALO_PEACE = INTERVALO_HUNTED  # compatibilidade
INTERVALO_LOOP = 5
INTERVALO_MONITOR = INTERVALO_GUILDA
HORA_ATUALIZACAO_DIARIA = 3
MINUTO_ATUALIZACAO_DIARIA = 0

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
THREADS = 10
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0"

# Mensagens comuns enviadas por webhook usam content e devem ficar abaixo de 2000.
DISCORD_LIMITE = 1950
LEVEL_IMPORTANTES = [600, 700, 800]
MARGEM_QUASE_LEVEL = 5
INATIVO_AVISO = 10
INATIVO_REMOCAO = 20
HISTORICO_LEVELS_MAX = 60
TAGS_VALIDAS = ["virtue", "culpa", "pravus"]

# =========================
# ALIASES DE COMPATIBILIDADE
# =========================
# Mantêm compatibilidade com versões anteriores dos módulos.
WEBHOOK_SAIDA_MEMBROS_HUNTED = WEBHOOK_SAIDA_MEMBROS
WEBHOOK_MOB_XP_HUNTED = WEBHOOK_MOB_XP
WEBHOOK_SPY_INFO_HUNTED = WEBHOOK_SPY_INFO
