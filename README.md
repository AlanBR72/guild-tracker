# Rucoy Guild Tracker

## Execução

```bash
pip install -r requirements.txt
python main.py
```

## Guildas hunted

Adicione ou remova guildas somente no `config.py`, dentro de `GUILDAS_HUNTED`:

```python
GUILDAS_HUNTED = {
    "Peace Killers": {
        "url": "https://www.rucoyonline.com/guild/Peace%20Killers",
    },
    "Infernal Cruelty": {
        "url": "https://www.rucoyonline.com/guild/Infernal%20Cruelty",
    },
}
```

Cada guilda possui históricos separados nos canais compartilhados:

- `#mob-xp`
- `#saída-membros`

Ao atingir o limite configurado em `DISCORD_LIMITE` (1950), o bot:

1. mantém a lista antiga no canal;
2. envia um aviso de limite;
3. cria uma nova lista para aquela guilda;
4. passa a editar somente a nova mensagem.

O canal `#spy-info` recebe um relatório diário com todas as guildas hunted configuradas.

## Painéis diários das guildas hunted

- Cada guilda configurada em `GUILDAS_HUNTED` possui um painel separado no `#spy-info`.
- Os ups ficam acumulados no painel da própria guilda durante o ciclo diário.
- O ciclo começa às 03:00 no horário de Brasília.
- Na virada, o bot mantém os painéis antigos e envia novos painéis zerados.
- O estado do ciclo, os IDs das mensagens e os ups são persistidos em JSON.
