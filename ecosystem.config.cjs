module.exports = {
  apps: [
    {
      name: 'castor-bot',
      script: 'index.js',
      cwd: '/root/bot-whats-inteligencia',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '700M',
      env: {
        NODE_ENV: 'production',
        PORT: 3000,
        RESET_WA_SESSION_ON_BOOT: 'false',
        LOCAL_STORE_FILE: '/root/bot-whats-inteligencia/data/castor_store.json',
        SAFE_MODE: 'true',
        ALLOW_SELF_COMMANDS: 'false',
        SEND_MIN_DELAY_MS: '1800',
        SEND_MAX_DELAY_MS: '4200',
        PER_CHAT_MIN_GAP_MS: '3600',
        BOT_HEALTHCHECK_INTERVAL_MS: '60000',
        BOT_STALE_SOCKET_MS: '240000',
        BOT_SUSPEND_DETECTION_MS: '60000',
        BOT_SUSPEND_CHECK_INTERVAL_MS: '5000',
        BOT_PRESENCE_KEEPALIVE_MS: '600000',
        BAILEYS_QUERY_TIMEOUT_MS: '60000',
        BAILEYS_CONNECT_TIMEOUT_MS: '60000',
        BAILEYS_KEEPALIVE_MS: '30000',
        BAILEYS_RETRY_REQUEST_DELAY_MS: '5000',
        SEND_ACTION_TIMEOUT_MS: '20000',
        PROACTIVE_ENABLED: 'true',
        PROACTIVE_GROUP_JID: '',
        PROACTIVE_PROMPT_INTERVAL_MS: '60000',
        PROACTIVE_RANDOM_USER_INTERVAL_MS: '60000',
        PROACTIVE_INACTIVITY_THRESHOLD_MS: '64800000',
        PROACTIVE_NIGHT_START_HOUR: '23',
        PROACTIVE_NIGHT_END_HOUR: '9',
        PROACTIVE_JITTER_MS: '5000',
        PROACTIVE_SHOWCASE_INTERVAL_MS: '60000',
        PROACTIVE_SHOWCASE_PROMPT_GAP_MS: '120000',
        GEMINI_API_KEY: 'AIzaSyBnDUmy7RCoOd8_16cOOPHkT-O-0DhD0-U'
      }
    }
  ]
}
