# Rucoy Guild Tracker

## Como executar

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

2. Execute:

```bash
python main.py
```

## Funcionamento

- A cada 10 minutos:
  - verifica entradas e saídas da Virtue;
  - verifica level ups, level downs e quase levels importantes;
  - envia mensagem nova somente quando houver mudança.

- Todos os dias às 03:00 no horário do Brasil:
  - atualiza o painel `#visao-geral`;
  - atualiza o painel `#spy-rank`;
  - atualiza o painel `#peace-killers`.

## Canais

- `#spy-rank`: painel editado.
- `#visao-geral`: painel editado.
- `#entrada-e-saidas`: nova mensagem quando houver mudança.
- `#up-levels`: nova mensagem quando houver mudança.
- `#peace-killers`: painel editado.

## Aviso de segurança

Os webhooks estão no `config.py`. Depois de testar, é recomendável recriar os webhooks no Discord e substituir os links.
