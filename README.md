# Rucoy Guild Tracker

## Como executar

```bash
pip install -r requirements.txt
python main.py
```

## Funcionamento atual

### 🛡️ STATUS GUILDA

- `#visao-geral`
  - cria a mensagem na primeira execução;
  - depois edita a mesma mensagem a cada 10 minutos.

### 🏆 TRACKER

- `#spy-rank`
  - cria uma mensagem na primeira execução;
  - depois cria uma nova mensagem todos os dias às 03:00.

### 🎯 TRACKER INIMIGOS

- `#peace-killers`
  - cria uma mensagem na primeira execução;
  - depois cria uma nova mensagem todos os dias às 03:00.

### Canais de histórico

- `#entrada-e-saidas`
  - verifica a cada 10 minutos;
  - envia nova mensagem somente se houver entrada ou saída.

- `#up-levels`
  - verifica a cada 10 minutos;
  - envia nova mensagem somente se houver level up, level down ou quase level.

## Arquivos locais

A pasta `data/` guarda os estados JSON usados pelo bot. Ela é criada automaticamente se não existir.

## Aviso de segurança

Os webhooks estão no `config.py`. Depois de testar, é recomendável recriar os webhooks no Discord e substituir os links.
