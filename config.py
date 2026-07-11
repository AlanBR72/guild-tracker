import pytz

# =========================
# URLs DO RUCOY
# =========================
GUILD_URL = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
# Guildas atualmente em hunted.
# Para adicionar/remover uma guilda, altere apenas este dicionário.
GUILDAS_HUNTED = {
    "peace_killers": {
        "nome": "Peace Killers",
        "url": "https://www.rucoyonline.com/guild/Peace%20Killers",
    },
    "infernal_cruelty": {
        "nome": "Infernal Cruelty",
        "url": "https://www.rucoyonline.com/guild/Infernal%20Cruelty",
    },
}

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
WEBHOOK_SPY_INFO = "https://discord.com/api/webhooks/1524443881045757983/rA2XNrujiBnp7lHh54b3MAkTTMesRbuqP9nEO44qjy7WP0e0jt_E8h7Hsu7etw1qiwyc"
WEBHOOK_MOB_XP = "https://discord.com/api/webhooks/1524793800688406642/OucCMAmMXTG-Pr3uWi9UoyS1rCIXiTHMlL4ASGZ199yiQ_ED5H5YP7hJ9bmGPrOlyU3-"
WEBHOOK_SAIDA_MEMBROS_HUNTED = "https://discord.com/api/webhooks/1525161252974760046/pSK7N_O_t75H2lVOKHHsMte2CuHEVRApMFxAggHaUAkeZNPrhglNVjCmbbL5OqZQ1BL6"

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

# =========================
# TEMPO / EXECUÇÃO
# =========================
BRASIL = pytz.timezone("America/Sao_Paulo")
INTERVALO_GUILDA = 600  # 10 minutos
INTERVALO_HUNTED = 300  # 5 minutos
INTERVALO_LOOP = 5      # checagem interna do loop
# compatibilidade com versões antigas
INTERVALO_MONITOR = INTERVALO_GUILDA
HORA_ATUALIZACAO_DIARIA = 3
MINUTO_ATUALIZACAO_DIARIA = 0

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
THREADS = 10
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0"
DISCORD_LIMITE = 1900  # webhooks em content aceitam até 2000; usamos margem
LEVEL_IMPORTANTES = [600, 700, 800]
MARGEM_QUASE_LEVEL = 5
INATIVO_AVISO = 10
INATIVO_REMOCAO = 20
HISTORICO_LEVELS_MAX = 60
TAGS_VALIDAS = ["virtue", "culpa", "pravus"]
