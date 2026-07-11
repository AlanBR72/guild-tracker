# Rucoy Guild Tracker

Bot de monitoramento da **Guilt Of Virtue** e das guildas configuradas como hunted.

## Guildas hunted configuráveis

Edite apenas `GUILDAS_HUNTED` em `config.py`:

```python
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
```

Para remover uma guilda, apague o bloco dela. Para adicionar, crie outro bloco com uma chave única, nome e URL.

## Intervalos

- Virtue: `INTERVALO_GUILDA = 600` (10 minutos)
- Guildas hunted: `INTERVALO_HUNTED = 300` (5 minutos)
- Painéis diários: 03:00, horário de Brasília

## Canais hunted

- `#spy-info`: relatório diário de todas as guildas configuradas.
- `#mob-xp`: uma mensagem/histórico separado para cada guilda.
- `#saída-membros`: uma mensagem/histórico separado para cada guilda.

Quando um histórico chega a `DISCORD_LIMITE` (1900), o bot envia um aviso e cria uma nova lista para somente aquela guilda. As outras listas continuam normalmente.

## Executar

```bash
pip install -r requirements.txt
python main.py
```
