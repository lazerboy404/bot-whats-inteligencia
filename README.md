# bot-whatsapp
Bot de WhatsApp avanzado con comandos de moderación, utilidades de grupo y branding Castor.

## VPS

Este proyecto ya está listo para correr en un VPS con Node.js y PM2.

### Requisitos

- Ubuntu 22.04 o similar
- Node.js 20
- npm
- Git
- PM2

### Instalación rápida

```bash
apt update && apt upgrade -y
apt install -y curl git
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs
npm install -g pm2
mkdir -p /root/bot-whats-inteligencia
cd /root
git clone https://github.com/lazerboy404/bot-whats-inteligencia.git
cd /root/bot-whats-inteligencia
npm install
cp .env.example .env
```

### Variables recomendadas para VPS

Este proyecto usa almacenamiento local en disco:

```env
PORT=3000
ADMIN_PHONE=5215564132674
RESET_WA_SESSION_ON_BOOT=true
LOCAL_STORE_FILE=/root/bot-whats-inteligencia/data/castor_store.json
SAFE_MODE=true
ALLOW_SELF_COMMANDS=false
SEND_MIN_DELAY_MS=1800
SEND_MAX_DELAY_MS=4200
PER_CHAT_MIN_GAP_MS=3600
BOT_HEALTHCHECK_INTERVAL_MS=60000
BOT_STALE_SOCKET_MS=240000
BOT_SUSPEND_DETECTION_MS=60000
BOT_SUSPEND_CHECK_INTERVAL_MS=5000
BOT_PRESENCE_KEEPALIVE_MS=600000
BAILEYS_QUERY_TIMEOUT_MS=60000
BAILEYS_CONNECT_TIMEOUT_MS=60000
BAILEYS_KEEPALIVE_MS=30000
BAILEYS_RETRY_REQUEST_DELAY_MS=5000
SEND_ACTION_TIMEOUT_MS=20000
```

### Inicio con PM2

```bash
cd /root/bot-whats-inteligencia
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

### Primer arranque

- Arranca con `RESET_WA_SESSION_ON_BOOT=true`
- Escanea el QR
- Cuando el bot conecte bien, cambia `.env` a `RESET_WA_SESSION_ON_BOOT=false`
- Reinicia con `pm2 restart castor-bot`

### Comandos útiles

```bash
pm2 logs castor-bot
pm2 restart castor-bot
pm2 stop castor-bot
pm2 status
```

### Archivos persistentes

- Sesión de WhatsApp: `auth_info_baileys/`
- Datos locales: `data/castor_store.json`

### Puertos

El bot expone una página simple en el puerto configurado en `PORT`. Si necesitas ver QR por navegador, abre el puerto 3000 en el firewall o usa Nginx como proxy.
