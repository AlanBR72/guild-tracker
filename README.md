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
  - depois edita a mesma mensagem a cada 5 minutos.

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
  - verifica a cada 5 minutos;
  - envia nova mensagem somente se houver entrada ou saída.

- `#up-levels`
  - na primeira execução salva os levels de todos e não envia alerta falso;
  - verifica a cada 5 minutos;
  - se houver level up, level down ou quase level, atualiza a mesma mensagem fixa;
  - mantém histórico em ordem cronológica: mais antigos em cima e mais recentes embaixo;
  - a seção de quase level só aparece quando houver alguém faltando 5 levels ou menos para 600, 700 ou 800.

## Arquivos locais

A pasta `data/` guarda os estados JSON usados pelo bot. Ela é criada automaticamente se não existir.

## Aviso de segurança

Os webhooks estão no `config.py`. Depois de testar, é recomendável recriar os webhooks no Discord e substituir os links.


## Mob XP Peace

O canal `#mob-xp-peace` monitora a Peace Killers a cada 5 minutos e registra somente membros que descerem de level. A primeira execução salva a base sem enviar alerta falso. Quando o histórico chega perto de 1900 caracteres, o bot cria uma nova mensagem e passa a editar essa nova mensagem.


## Novo canal Peace
- `#saída-membros-peace`: histórico de entradas, saídas e trocas de nick, atualizado a cada 5 minutos.
