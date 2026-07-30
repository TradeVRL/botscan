BOT-BUSCADOR-PRECO — DEPLOY SIMPLIFICADO

ARQUIVOS NECESSÁRIOS NA RAIZ DO GITHUB
- bot_busca_preco.py
- requirements.txt
- render.yaml
- .gitignore
- .python-version

O arquivo .env.example é apenas referência. Não envie credenciais reais ao GitHub.

DEPLOY NO RENDER
1. Envie os arquivos para a raiz de um repositório privado no GitHub.
2. No Render, escolha New > Blueprint.
3. Selecione o repositório e aplique o render.yaml.
4. Preencha as variáveis secretas solicitadas:
   TELEGRAM_BOT_TOKEN
   TELEGRAM_WEBHOOK_SECRET
   TELEGRAM_BOT_USERNAME
   ADMIN_USER_IDS
   SUPPORT_CONTACT
   SALES_MESSAGE
   LICENSE_PAYMENT_URL, se já houver checkout
   PROJECT_API_KEY
   OPENAI_API_KEY
5. DATABASE_URL será vinculada automaticamente ao PostgreSQL.
6. Aguarde o log APPLICATION_READY.
7. Teste https://SEU-SERVICO.onrender.com/health.
8. Abra o bot no Telegram e envie /start.

GERAR SEGREDOS
python -c "import secrets; print(secrets.token_urlsafe(32))"

Use uma saída diferente para TELEGRAM_WEBHOOK_SECRET e PROJECT_API_KEY.

COMANDOS ADMINISTRATIVOS
/admin_licenca ID dias
/admin_bloquear ID
/admin_desbloquear ID
/admin_creditos ID quantidade
/admin_stats

ARQUITETURA
- Um único Web Service no Render.
- Um único arquivo Python contém API, Telegram, fila, banco e pesquisa.
- PostgreSQL mantém usuários, histórico, franquias e pesquisas pendentes.
- Não usa Celery, Kombu, Redis ou Background Worker separado.

LIMITAÇÃO IMPORTANTE
O processamento interno pressupõe uma única instância do Web Service. Não aumente a escala horizontal para mais de uma instância sem separar o worker.
