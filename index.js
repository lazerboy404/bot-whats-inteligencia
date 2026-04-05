require('dotenv').config();
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers, downloadContentFromMessage, makeCacheableSignalKeyStore } = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;

const LOCAL_STORE_FILE = process.env.LOCAL_STORE_FILE || path.join(process.cwd(), 'data', 'castor_store.json');
const ADMIN_PHONE = process.env.ADMIN_PHONE || '5215564132674';
const ADMIN_NUMBER_VARIANTS = new Set(['5564132674', '525564132674', '5215564132674']);
const reportCooldownByUser = new Map();
const reportReferenceMap = new Map();
let isStorageReady = false;
let localStoreCache = null;
const GROUP_INVITE_REGEX = /(chat\.whatsapp\.com\/[a-zA-Z0-9]{20,}|wa\.me\/joinlink\/)/i;
let keepAliveInterval = null;
let healthWatchInterval = null;
let antiSleepInterval = null;
let presenceKeepAliveInterval = null;
const SAFE_MODE = ['1', 'true', 'yes', 'on'].includes(String(process.env.SAFE_MODE || '').toLowerCase());
const SEND_MIN_DELAY_MS = Number(process.env.SEND_MIN_DELAY_MS || (SAFE_MODE ? 1800 : 600));
const SEND_MAX_DELAY_MS = Number(process.env.SEND_MAX_DELAY_MS || (SAFE_MODE ? 4200 : 1800));
const PER_CHAT_MIN_GAP_MS = Number(process.env.PER_CHAT_MIN_GAP_MS || (SAFE_MODE ? 3600 : 1400));
const KEEP_ALIVE_INTERVAL_MS = Number(process.env.KEEP_ALIVE_INTERVAL_MS || (10 * 60 * 1000));
const RESET_WA_SESSION_ON_BOOT = ['1', 'true', 'yes', 'on'].includes(String(process.env.RESET_WA_SESSION_ON_BOOT || 'false').toLowerCase());
const SAFE_DISABLE_SEAL_STICKER = ['1', 'true', 'yes', 'on'].includes(String(process.env.SAFE_DISABLE_SEAL_STICKER || (SAFE_MODE ? 'true' : 'false')).toLowerCase());
const SAFE_DISABLE_COMMAND_REACT = ['1', 'true', 'yes', 'on'].includes(String(process.env.SAFE_DISABLE_COMMAND_REACT || 'false').toLowerCase());
const SAFE_DISABLE_AUTO_KICK = ['1', 'true', 'yes', 'on'].includes(String(process.env.SAFE_DISABLE_AUTO_KICK || (SAFE_MODE ? 'true' : 'false')).toLowerCase());
const SAFE_COMPACT_WELCOME = ['1', 'true', 'yes', 'on'].includes(String(process.env.SAFE_COMPACT_WELCOME || 'false').toLowerCase());
const ALLOW_SELF_COMMANDS = ['1', 'true', 'yes', 'on'].includes(String(process.env.ALLOW_SELF_COMMANDS || 'false').toLowerCase());
const lastSentAtByJid = new Map();
let globalSendQueue = Promise.resolve();
const closeTimersByGroup = new Map();
const scheduledGroupActionsByGroup = new Map();
const pendingWelcomesByGroup = new Map();
let reconnectTimer = null;
let reconnectInProgress = false;
let reconnectDelayMs = 4000;
let processErrorGuardReady = false;
let activeSock = null;
let isStartingBot = false;
let botRunId = 0;
let sessionResetDoneThisBoot = false;
let startupShowcaseSentRunId = 0;
let lastSocketActivityAt = Date.now();
let lastCommandHandledAt = 0;
const processedMessageIds = new Set();
const incomingQueue = [];
let isProcessingIncoming = false;
let incomingBufferTimeout = null;
const CASTOR_EMOJI = '🦫';
const CASTOR_DEFAULT_IMAGE_URL = process.env.CASTOR_DEFAULT_IMAGE_URL || 'https://raw.githubusercontent.com/lazerboy404/bot-whats-inteligencia/main/bienvenida.png';
const CASTOR_SEAL_STICKER_URL = process.env.CASTOR_SEAL_STICKER_URL || '';
const CASTOR_VALID_COMMANDS = new Set(['.reportar', '.advertir', '.ban', '.unban', '.sticker', '.fantasmas', '.cerrar', '.abrir', '.ping', '.top', '.random', '.comandos', '.reglas', '.miid', '.setadmin', '.troncos', '.dinamica', '.grupoid']);
const POSITIVE_REACTION_EMOJIS = new Set(['👍', '❤️', '👏', '🤯', '🔥', '💯', '🧠', '🤖', '🦫', '💡']);
const BAILEYS_QUERY_TIMEOUT_MS = Number(process.env.BAILEYS_QUERY_TIMEOUT_MS || 60000);
const BAILEYS_CONNECT_TIMEOUT_MS = Number(process.env.BAILEYS_CONNECT_TIMEOUT_MS || 60000);
const BAILEYS_KEEPALIVE_MS = Number(process.env.BAILEYS_KEEPALIVE_MS || 30000);
const BAILEYS_RETRY_REQUEST_DELAY_MS = Number(process.env.BAILEYS_RETRY_REQUEST_DELAY_MS || 5000);
const SEND_ACTION_TIMEOUT_MS = Number(process.env.SEND_ACTION_TIMEOUT_MS || 20000);
const BOT_HEALTHCHECK_INTERVAL_MS = Number(process.env.BOT_HEALTHCHECK_INTERVAL_MS || 60000);
const BOT_STALE_SOCKET_MS = Number(process.env.BOT_STALE_SOCKET_MS || 240000);
const BOT_SUSPEND_DETECTION_MS = Number(process.env.BOT_SUSPEND_DETECTION_MS || 60000);
const BOT_SUSPEND_CHECK_INTERVAL_MS = Number(process.env.BOT_SUSPEND_CHECK_INTERVAL_MS || 5000);
const BOT_PRESENCE_KEEPALIVE_MS = Number(process.env.BOT_PRESENCE_KEEPALIVE_MS || (10 * 60 * 1000));
const BOT_RECONNECT_BASE_MS = Number(process.env.BOT_RECONNECT_BASE_MS || 4000);
const BOT_RECONNECT_MAX_MS = Number(process.env.BOT_RECONNECT_MAX_MS || 45000);
const PROACTIVE_ENABLED = !['0', 'false', 'no', 'off'].includes(String(process.env.PROACTIVE_ENABLED || 'true').toLowerCase());
const PROACTIVE_GROUP_JIDS = (process.env.PROACTIVE_GROUP_JID || '').split(',').map(s => s.trim()).filter(Boolean);
const PROACTIVE_PROMPT_INTERVAL_MS = Number(process.env.PROACTIVE_PROMPT_INTERVAL_MS || (60 * 1000));
const PROACTIVE_RANDOM_USER_INTERVAL_MS = Number(process.env.PROACTIVE_RANDOM_USER_INTERVAL_MS || (24 * 60 * 60 * 1000));
const PROACTIVE_INACTIVITY_THRESHOLD_MS = Number(process.env.PROACTIVE_INACTIVITY_THRESHOLD_MS || (18 * 60 * 60 * 1000));
const PROACTIVE_NIGHT_START_HOUR = Number(process.env.PROACTIVE_NIGHT_START_HOUR || 23);
const PROACTIVE_NIGHT_END_HOUR = Number(process.env.PROACTIVE_NIGHT_END_HOUR || 9);
const PROACTIVE_JITTER_MS = Number(process.env.PROACTIVE_JITTER_MS || (5 * 1000));
const PROACTIVE_SHOWCASE_INTERVAL_MS = Number(process.env.PROACTIVE_SHOWCASE_INTERVAL_MS || (60 * 1000));
const PROACTIVE_SHOWCASE_PROMPT_GAP_MS = Number(process.env.PROACTIVE_SHOWCASE_PROMPT_GAP_MS || (2 * 60 * 1000));
const PROACTIVE_SEND_SHOWCASE_ON_START = !['0', 'false', 'no', 'off'].includes(String(process.env.PROACTIVE_SEND_SHOWCASE_ON_START || 'true').toLowerCase());
const SHOWCASE_REPOS = [
    {
        id: 'picotrex',
        url: 'https://raw.githubusercontent.com/PicoTrex/Awesome-Nano-Banana-images/main/README_en.md',
        imageBaseUrl: 'https://raw.githubusercontent.com/PicoTrex/Awesome-Nano-Banana-images/main/'
    },
    {
        id: 'jimmylv',
        url: 'https://raw.githubusercontent.com/JimmyLv/awesome-nano-banana/main/README.md',
        imageBaseUrl: 'https://raw.githubusercontent.com/JimmyLv/awesome-nano-banana/main/'
    },
    {
        id: 'supermaker',
        url: 'https://raw.githubusercontent.com/Super-Maker-AI/awesome-nano-banana/main/README.md',
        imageBaseUrl: 'https://raw.githubusercontent.com/Super-Maker-AI/awesome-nano-banana/main/'
    },
    {
        id: 'zerolu',
        url: 'https://raw.githubusercontent.com/ZeroLu/awesome-nanobanana-pro/main/README.md',
        imageBaseUrl: 'https://raw.githubusercontent.com/ZeroLu/awesome-nanobanana-pro/main/'
    }
];
const SHOWCASE_PROMPT_INLINE_MAX_LENGTH = 500;
const FLAG_BY_DIAL_CODE = {
    '1': '🇺🇸',
    '34': '🇪🇸',
    '52': '🇲🇽',
    '521': '🇲🇽',
    '54': '🇦🇷',
    '55': '🇧🇷',
    '56': '🇨🇱',
    '57': '🇨🇴',
    '58': '🇻🇪',
    '51': '🇵🇪',
    '44': '🇬🇧',
    '49': '🇩🇪'
};

const COUNTRY_BY_DIAL_CODE = {
    '1': 'Estados Unidos/Canadá',
    '7': 'Rusia/Kazajistán',
    '20': 'Egipto',
    '27': 'Sudáfrica',
    '30': 'Grecia',
    '31': 'Países Bajos',
    '32': 'Bélgica',
    '33': 'Francia',
    '34': 'España',
    '39': 'Italia',
    '40': 'Rumania',
    '41': 'Suiza',
    '43': 'Austria',
    '44': 'Reino Unido',
    '45': 'Dinamarca',
    '46': 'Suecia',
    '47': 'Noruega',
    '48': 'Polonia',
    '49': 'Alemania',
    '51': 'Perú',
    '52': 'México',
    '53': 'Cuba',
    '54': 'Argentina',
    '55': 'Brasil',
    '56': 'Chile',
    '57': 'Colombia',
    '58': 'Venezuela',
    '60': 'Malasia',
    '61': 'Australia',
    '62': 'Indonesia',
    '63': 'Filipinas',
    '64': 'Nueva Zelanda',
    '65': 'Singapur',
    '66': 'Tailandia',
    '81': 'Japón',
    '82': 'Corea del Sur',
    '84': 'Vietnam',
    '86': 'China',
    '90': 'Turquía',
    '91': 'India',
    '92': 'Pakistán',
    '93': 'Afganistán',
    '94': 'Sri Lanka',
    '95': 'Myanmar',
    '98': 'Irán',
    '211': 'Sudán del Sur',
    '212': 'Marruecos',
    '213': 'Argelia',
    '216': 'Túnez',
    '218': 'Libia',
    '220': 'Gambia',
    '221': 'Senegal',
    '222': 'Mauritania',
    '223': 'Malí',
    '224': 'Guinea',
    '225': 'Costa de Marfil',
    '226': 'Burkina Faso',
    '227': 'Níger',
    '228': 'Togo',
    '229': 'Benín',
    '230': 'Mauricio',
    '231': 'Liberia',
    '232': 'Sierra Leona',
    '233': 'Ghana',
    '234': 'Nigeria',
    '235': 'Chad',
    '236': 'República Centroafricana',
    '237': 'Camerún',
    '238': 'Cabo Verde',
    '239': 'Santo Tomé y Príncipe',
    '240': 'Guinea Ecuatorial',
    '241': 'Gabón',
    '242': 'República del Congo',
    '243': 'República Democrática del Congo',
    '244': 'Angola',
    '245': 'Guinea-Bisáu',
    '246': 'Territorio Británico del Océano Índico',
    '248': 'Seychelles',
    '249': 'Sudán',
    '250': 'Ruanda',
    '251': 'Etiopía',
    '252': 'Somalia',
    '253': 'Yibuti',
    '254': 'Kenia',
    '255': 'Tanzania',
    '256': 'Uganda',
    '257': 'Burundi',
    '258': 'Mozambique',
    '260': 'Zambia',
    '261': 'Madagascar',
    '262': 'Reunión/Mayotte',
    '263': 'Zimbabue',
    '264': 'Namibia',
    '265': 'Malaui',
    '266': 'Lesoto',
    '267': 'Botsuana',
    '268': 'Esuatini',
    '269': 'Comoras',
    '290': 'Santa Elena',
    '291': 'Eritrea',
    '297': 'Aruba',
    '298': 'Islas Feroe',
    '299': 'Groenlandia',
    '350': 'Gibraltar',
    '351': 'Portugal',
    '352': 'Luxemburgo',
    '353': 'Irlanda',
    '354': 'Islandia',
    '355': 'Albania',
    '356': 'Malta',
    '357': 'Chipre',
    '358': 'Finlandia',
    '359': 'Bulgaria',
    '370': 'Lituania',
    '371': 'Letonia',
    '372': 'Estonia',
    '373': 'Moldavia',
    '374': 'Armenia',
    '375': 'Bielorrusia',
    '376': 'Andorra',
    '377': 'Mónaco',
    '378': 'San Marino',
    '380': 'Ucrania',
    '381': 'Serbia',
    '382': 'Montenegro',
    '383': 'Kosovo',
    '385': 'Croacia',
    '386': 'Eslovenia',
    '387': 'Bosnia y Herzegovina',
    '389': 'Macedonia del Norte',
    '420': 'República Checa',
    '421': 'Eslovaquia',
    '423': 'Liechtenstein',
    '500': 'Islas Malvinas',
    '501': 'Belice',
    '502': 'Guatemala',
    '503': 'El Salvador',
    '504': 'Honduras',
    '505': 'Nicaragua',
    '506': 'Costa Rica',
    '507': 'Panamá',
    '508': 'San Pedro y Miquelón',
    '509': 'Haití',
    '590': 'Guadalupe/San Martín/San Bartolomé',
    '591': 'Bolivia',
    '592': 'Guyana',
    '593': 'Ecuador',
    '594': 'Guayana Francesa',
    '595': 'Paraguay',
    '596': 'Martinica',
    '597': 'Surinam',
    '598': 'Uruguay',
    '599': 'Curazao/Caribe Neerlandés',
    '670': 'Timor Oriental',
    '672': 'Territorios Australianos Externos',
    '673': 'Brunéi',
    '674': 'Nauru',
    '675': 'Papúa Nueva Guinea',
    '676': 'Tonga',
    '677': 'Islas Salomón',
    '678': 'Vanuatu',
    '679': 'Fiyi',
    '680': 'Palaos',
    '681': 'Wallis y Futuna',
    '682': 'Islas Cook',
    '683': 'Niue',
    '685': 'Samoa',
    '686': 'Kiribati',
    '687': 'Nueva Caledonia',
    '688': 'Tuvalu',
    '689': 'Polinesia Francesa',
    '690': 'Tokelau',
    '691': 'Micronesia',
    '692': 'Islas Marshall',
    '850': 'Corea del Norte',
    '852': 'Hong Kong',
    '853': 'Macao',
    '855': 'Camboya',
    '856': 'Laos',
    '880': 'Bangladés',
    '886': 'Taiwán',
    '960': 'Maldivas',
    '961': 'Líbano',
    '962': 'Jordania',
    '963': 'Siria',
    '964': 'Irak',
    '965': 'Kuwait',
    '966': 'Arabia Saudita',
    '967': 'Yemen',
    '968': 'Omán',
    '970': 'Palestina',
    '971': 'Emiratos Árabes Unidos',
    '972': 'Israel',
    '973': 'Baréin',
    '974': 'Catar',
    '975': 'Bután',
    '976': 'Mongolia',
    '977': 'Nepal',
    '992': 'Tayikistán',
    '993': 'Turkmenistán',
    '994': 'Azerbaiyán',
    '995': 'Georgia',
    '996': 'Kirguistán',
    '998': 'Uzbekistán'
};

const SORTED_DIAL_CODES = Object.keys(COUNTRY_BY_DIAL_CODE).sort((a, b) => b.length - a.length);

const PROACTIVE_PROMPT_CHALLENGES = [
    { emoji: '🎨', topic: 'Generación de Imágenes', challenge: 'generar una imagen de un robot futurista estilo cyberpunk en una ciudad lluviosa de noche' },
    { emoji: '✍️', topic: 'Mejora de Prompt', challenge: 'mejorar este prompt básico: "Hazme un resumen de este texto" → hazlo más preciso, con formato y contexto' },
    { emoji: '🤖', topic: 'System Prompt', challenge: 'crear un system prompt para un chatbot de atención al cliente de una tienda de tecnología' },
    { emoji: '💻', topic: 'Generación de Código', challenge: 'crear un prompt que le pida a la IA generar una función en Python que ordene una lista de diccionarios por múltiples campos' },
    { emoji: '📊', topic: 'Análisis de Datos', challenge: 'escribir un prompt para que la IA analice datos de ventas mensuales y detecte tendencias y anomalías' },
    { emoji: '🧠', topic: 'Chain of Thought', challenge: 'crear un prompt que use razonamiento paso a paso (chain-of-thought) para resolver un problema de lógica' },
    { emoji: '🏔️', topic: 'Arte con IA', challenge: 'generar una imagen de un paisaje de fantasía con montañas flotantes, cascadas y dragones al atardecer' },
    { emoji: '📧', topic: 'Escritura Profesional', challenge: 'crear un prompt para redactar un correo profesional pidiendo un aumento de sueldo de forma convincente' },
    { emoji: '🌍', topic: 'Traducción Inteligente', challenge: 'escribir un prompt para traducir un texto del inglés al español manteniendo modismos y contexto cultural' },
    { emoji: '📖', topic: 'Storytelling', challenge: 'crear un prompt para generar una historia corta de ciencia ficción con un giro inesperado al final' },
    { emoji: '🐛', topic: 'Debugging con IA', challenge: 'escribir un prompt para que la IA te ayude a encontrar y corregir bugs en un fragmento de código' },
    { emoji: '📸', topic: 'Fotografía con IA', challenge: 'generar una imagen de un retrato fotorrealista estilo cinematográfico con iluminación dramática' },
    { emoji: '📚', topic: 'Plan de Estudios', challenge: 'crear un prompt para que la IA genere un plan de estudios de 30 días para aprender los fundamentos de IA' },
    { emoji: '🔍', topic: 'Extracción de Datos', challenge: 'escribir un prompt para extraer información estructurada (nombre, fecha, monto) de un texto desordenado' },
    { emoji: '📱', topic: 'Marketing con IA', challenge: 'crear un prompt para generar 5 ideas de contenido para redes sociales de una marca de tecnología' },
    { emoji: '🎵', topic: 'Prompt Creativo', challenge: 'escribir un prompt para que la IA componga la letra de una canción sobre inteligencia artificial en estilo reggaetón' },
    { emoji: '🏗️', topic: 'Arquitectura de Prompts', challenge: 'diseñar un mega-prompt con rol, contexto, instrucciones, formato de salida y ejemplos para resumir artículos científicos' }
];

const PROACTIVE_USER_TOPICS = [
    '¿Cuál es tu herramienta de IA favorita y por qué? 🤖',
    'Comparte un prompt que te haya funcionado increíble ✨',
    '¿Para qué usas la IA en tu día a día? 💡',
    'Cuéntanos tu mejor experiencia usando IA 🧠',
    '¿Qué modelo de IA prefieres? ChatGPT, Claude, Gemini...? 🤔',
    '¿Has usado IA para generar imágenes? Comparte tu experiencia 🎨',
    '¿Cuál es el prompt más creativo que has usado? 🎯',
    '¿Qué tarea te ha resuelto la IA que antes te costaba mucho? ⚡',
    '¿Conoces algún truco de prompting que quieras compartir? 🔑',
    '¿Cómo ves el futuro de la IA en tu campo laboral? 🔮',
    '¿Qué app o herramienta de IA descubriste recientemente? 📱',
    '¿Has automatizado algo con IA? Cuéntanos cómo 🤖'
];

const PROACTIVE_REACTIVATION_MESSAGES = [
    '¿Siguen vivos o ya se secó el dique? 😴\n\nCompartan algo de IA para revivir el estanque 👇',
    'Detecto poca actividad...\n\nCompartan un prompt que hayan usado hoy 👇',
    'El estanque está muy tranquilo... 🌊\n\n¿Alguien ha probado alguna herramienta de IA nueva?',
    '¡Castores! El dique necesita actividad 🪵\n\n¿Qué es lo último que le pidieron a una IA?',
    'Aquí no ha pasado nada en un buen rato...\n\n¿Alguien tiene un prompt interesante para compartir? 🤖',
    'El río está detenido 🌿\n\nCuéntenme: ¿para qué usaron IA hoy?',
    '¿Se quedaron sin prompts? 🤔\n\nTiren uno que les haya funcionado esta semana',
    'El grupo necesita leña... digo, troncos 🪵\n\n¡Compartan algo sobre IA!',
    'Esto está más callado que servidor sin internet 🔇\n\n¿Nadie tiene un descubrimiento de IA que compartir?',
    'Últiiiima llamada para castores activos 📢\n\n¿Qué herramienta de IA han estado usando?'
];

let lastGroupActivityAt = 0;
let proactiveCheckInterval = null;
let proactiveCheckRunning = false;
let showcaseCacheData = null;
let showcaseCacheTimestamp = 0;

setInterval(() => {
    processedMessageIds.clear();
}, 60 * 60 * 1000);

setInterval(() => {
    cleanOldReactionRecords();
}, 6 * 60 * 60 * 1000);

function cleanDigits(value) {
    return String(value || '').replace(/\D/g, '');
}

function getDefaultLocalStore() {
    return {
        modRecords: {},
        messageReactions: {},
        botConfig: {
            adminPrivateJid: '',
            adminSenderJid: '',
            updatedAt: null
        },
        proactiveState: {}
    };
}

function ensureLocalStoreLoaded() {
    if (localStoreCache) {
        return localStoreCache;
    }
    try {
        const raw = fs.readFileSync(LOCAL_STORE_FILE, 'utf8');
        localStoreCache = { ...getDefaultLocalStore(), ...JSON.parse(raw || '{}') };
    } catch (error) {
        localStoreCache = getDefaultLocalStore();
    }
    return localStoreCache;
}

function saveLocalStore() {
    const store = ensureLocalStoreLoaded();
    fs.mkdirSync(path.dirname(LOCAL_STORE_FILE), { recursive: true });
    fs.writeFileSync(LOCAL_STORE_FILE, JSON.stringify(store, null, 2), 'utf8');
}

function applyUpdateObject(target, update) {
    const next = { ...(target || {}) };
    if (update?.$set) {
        Object.assign(next, update.$set);
    }
    if (update?.$inc) {
        for (const [key, value] of Object.entries(update.$inc)) {
            next[key] = Number(next[key] || 0) + Number(value || 0);
        }
    }
    if (update?.$push) {
        for (const [key, value] of Object.entries(update.$push)) {
            const current = Array.isArray(next[key]) ? [...next[key]] : [];
            current.push(value);
            next[key] = current;
        }
    }
    return next;
}

function normalizePhoneForCompare(value) {
    let digits = cleanDigits(value);
    if (digits.startsWith('00')) {
        digits = digits.replace(/^00+/, '');
    }
    if (digits.startsWith('0521')) {
        digits = `52${digits.slice(4)}`;
    }
    if (digits.startsWith('521')) {
        digits = `52${digits.slice(3)}`;
    }
    return digits;
}

function getNumberFromJid(jid) {
    return cleanDigits(String(jid || '').split('@')[0].split(':')[0]);
}

function getDomainFromJid(jid) {
    return String(jid || '').split('@')[1]?.toLowerCase() || '';
}

function toJid(number) {
    return `${cleanDigits(number)}@s.whatsapp.net`;
}

function getAdminJid() {
    const base = cleanDigits(ADMIN_PHONE);
    if (base.startsWith('521') && base.length >= 13) {
        return toJid(base);
    }
    if (base.startsWith('52') && base.length >= 12) {
        return toJid(base);
    }
    if (base.length === 10) {
        return toJid(`52${base}`);
    }
    return toJid(base);
}

function getAdminJidCandidates() {
    const candidates = new Set();
    candidates.add(getAdminJid());
    for (const variant of ADMIN_NUMBER_VARIANTS) {
        const digits = cleanDigits(variant);
        if (digits) {
            candidates.add(toJid(digits));
        }
    }
    return [...candidates];
}

async function getSavedAdminJidCandidates() {
    const config = await getSavedAdminConfig();
    if (!config) {
        return [];
    }
    const candidates = [];
    if (config?.adminPrivateJid) {
        candidates.push(config.adminPrivateJid);
    }
    if (config?.adminSenderJid && config.adminSenderJid !== config.adminPrivateJid) {
        candidates.push(config.adminSenderJid);
    }
    return candidates;
}

async function saveAdminPrivateJids(privateJid, senderJid) {
    if (!isStorageReady) {
        return false;
    }
    await saveBotConfigRecord(
        {
            $set: {
                adminPrivateJid: sanitizeText(privateJid || '', 160),
                adminSenderJid: sanitizeText(senderJid || '', 160),
                updatedAt: new Date()
            }
        }
    );
    return true;
}

async function getSavedAdminConfig() {
    if (!isStorageReady) {
        return null;
    }
    const store = ensureLocalStoreLoaded();
    return store.botConfig ? { ...store.botConfig } : null;
}

async function resolveAdminJids(sock) {
    const candidates = [...new Set([...(await getSavedAdminJidCandidates()), ...getAdminJidCandidates()])];
    const digits = [...new Set(candidates.map((jid) => cleanDigits(jid)))].filter(Boolean);
    const resolvedJids = new Set();

    try {
        const matches = await sock.onWhatsApp(...digits);
        for (const match of matches || []) {
            if (match?.exists && match?.jid) {
                resolvedJids.add(match.jid);
            }
        }
    } catch (error) {
    }

    if (resolvedJids.size > 0) {
        return [...resolvedJids];
    }
    return candidates;
}

async function canManageAdminLink(sock, msg, remoteJid) {
    const senderJid = msg.key.participant || msg.key.remoteJid || '';
    const senderNumber = getNumberFromJid(senderJid);
    if (isOwnerByNumber(senderNumber)) {
        return true;
    }
    const savedConfig = await getSavedAdminConfig();
    if (!savedConfig?.adminPrivateJid && !savedConfig?.adminSenderJid) {
        return true;
    }
    if (!remoteJid) {
        return false;
    }
    return savedConfig.adminPrivateJid === remoteJid
        || savedConfig.adminPrivateJid === senderJid
        || savedConfig.adminSenderJid === remoteJid
        || savedConfig.adminSenderJid === senderJid;
}

function isOwnerByNumber(number) {
    const normalized = normalizePhoneForCompare(number);
    return ADMIN_NUMBER_VARIANTS.has(normalized) || ADMIN_NUMBER_VARIANTS.has(cleanDigits(number));
}

function getCountryFromNumber(number) {
    const normalized = normalizePhoneForCompare(number);
    if (normalized.startsWith('52')) {
        return 'México';
    }
    for (const code of SORTED_DIAL_CODES) {
        if (normalized.startsWith(code)) {
            return COUNTRY_BY_DIAL_CODE[code];
        }
    }
    return 'un país no identificado';
}

function getFlagFromNumber(number) {
    const normalized = normalizePhoneForCompare(number);
    if (normalized.startsWith('52')) {
        return '🇲🇽';
    }
    for (const code of SORTED_DIAL_CODES) {
        if (normalized.startsWith(code)) {
            return FLAG_BY_DIAL_CODE[code] || '🌍';
        }
    }
    return '🌍';
}

function sanitizeText(value, maxLength = 3500) {
    const plain = String(value ?? '')
        .replace(/[^\x09\x0A\x0D\x20-\x7E\u00A0-\u024F\u1E00-\u1EFF]/g, '')
        .trim();
    if (plain.length <= maxLength) {
        return plain;
    }
    return `${plain.slice(0, maxLength)}...`;
}

function hasGroupInviteLink(text) {
    return GROUP_INVITE_REGEX.test(String(text || ''));
}

function getCastorSignatureText() {
    const dateText = new Intl.DateTimeFormat('es-MX', {
        day: 'numeric',
        month: 'long',
        timeZone: 'America/Mexico_City'
    }).format(new Date());
    return `> 𝗖𝗮𝘀𝘁𝗼𝗿 𝗕𝗼𝘁 ${CASTOR_EMOJI} | ${dateText}`;
}

function formatMexicoDateTime(value) {
    return new Intl.DateTimeFormat('es-MX', {
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: 'America/Mexico_City'
    }).format(value);
}

function brandCastorText(value) {
    let text = String(value ?? '').trim();
    if (text.length > 9000) {
        text = `${text.slice(0, 9000)}...`;
    }
    if (!text) {
        text = `${CASTOR_EMOJI} Castor Bot al habla.`;
    }
    if (!text.includes(CASTOR_EMOJI)) {
        text = `${CASTOR_EMOJI} ${text}`;
    }
    if (!/(dique|estanque|presa|corriente|𝗖𝗮𝘀𝘁𝗼𝗿 𝗕𝗼𝘁|Castor Bot \ud83e\uddab)/i.test(text)) {
        text = `${text}\n\n${getCastorSignatureText()}`;
    }
    return text;
}

async function streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}

function getRandomDelay(minMs, maxMs) {
    const safeMin = Math.max(0, Number(minMs) || 0);
    const safeMax = Math.max(safeMin, Number(maxMs) || safeMin);
    return Math.floor(Math.random() * (safeMax - safeMin + 1)) + safeMin;
}

function getMainMessageObject(message) {
    if (!message) return null;
    if (message.ephemeralMessage?.message) return message.ephemeralMessage.message;
    if (message.viewOnceMessage?.message) return message.viewOnceMessage.message;
    if (message.viewOnceMessageV2?.message) return message.viewOnceMessageV2.message;
    if (message.viewOnceMessageV2Extension?.message) return message.viewOnceMessageV2Extension.message;
    return message;
}

function extractTextFromMessage(message) {
    const body = getMainMessageObject(message);
    if (!body) return '';
    if (body.conversation) return sanitizeText(body.conversation);
    if (body.extendedTextMessage?.text) return sanitizeText(body.extendedTextMessage.text);
    if (body.imageMessage?.caption) return sanitizeText(body.imageMessage.caption);
    if (body.videoMessage?.caption) return sanitizeText(body.videoMessage.caption);
    if (body.documentMessage?.caption) return sanitizeText(body.documentMessage.caption);
    if (body.buttonsResponseMessage?.selectedButtonId) return sanitizeText(body.buttonsResponseMessage.selectedButtonId);
    if (body.listResponseMessage?.singleSelectReply?.selectedRowId) return sanitizeText(body.listResponseMessage.singleSelectReply.selectedRowId);
    return '';
}

function getContextInfoFromMessage(message) {
    const body = getMainMessageObject(message);
    if (!body) return null;
    const type = Object.keys(body)[0];
    if (!type) return null;
    return body[type]?.contextInfo || null;
}

function getQuotedPayload(message) {
    const contextInfo = getContextInfoFromMessage(message);
    if (!contextInfo) return null;
    return {
        contextInfo,
        quotedMessage: contextInfo.quotedMessage || null,
        quotedParticipant: contextInfo.participant || null,
        quotedStanzaId: contextInfo.stanzaId || null
    };
}

function describeQuotedContent(quotedMessage) {
    const payload = getMainMessageObject(quotedMessage);
    if (!payload) return { mediaType: 'desconocido', text: '', raw: '' };
    const mediaType = Object.keys(payload)[0] || 'desconocido';
    const text = extractTextFromMessage(payload);
    const raw = sanitizeText(JSON.stringify(payload));
    return { mediaType, text, raw };
}

function parseTargetFromTextOrMention(msg, text) {
    const contextInfo = getContextInfoFromMessage(msg.message);
    const mentioned = contextInfo?.mentionedJid || [];
    if (mentioned.length > 0) {
        return mentioned[0];
    }

    const arg = sanitizeText(text).split(/\s+/).slice(1).find(Boolean);
    if (!arg) return null;
    if (arg.includes('@')) {
        return arg;
    }
    const digits = cleanDigits(arg);
    if (digits.length >= 8) {
        return toJid(digits);
    }
    return null;
}

function parseTargetFromReportText(text) {
    const match = String(text || '').match(/ID infractor:\s*([^\s]+)/i);
    if (match?.[1]) return match[1];
    return null;
}

function parseGroupFromReportText(text) {
    const match = String(text || '').match(/Grupo:\s*([0-9\-]+@g\.us)/i);
    if (match?.[1]) return match[1];
    return null;
}

function parseCloseDurationMs(rawText) {
    const input = sanitizeText(rawText || '').toLowerCase();
    if (!input) {
        return null;
    }

    const match = input.match(/(\d+)\s*(h|hr|hrs|hora|horas|m|min|mins|minuto|minutos)\b/i);
    if (!match) {
        return null;
    }

    const amount = Number(match[1]);
    if (!Number.isFinite(amount) || amount <= 0) {
        return null;
    }

    const unit = match[2].toLowerCase();
    if (['h', 'hr', 'hrs', 'hora', 'horas'].includes(unit)) {
        return amount * 60 * 60 * 1000;
    }
    return amount * 60 * 1000;
}

function parseFutureActionTime(rawText) {
    const input = sanitizeText(rawText || '').toLowerCase();
    if (!input) {
        return null;
    }

    const relativeMatch = input.match(/^en\s+(\d+)\s*(h|hr|hrs|hora|horas|m|min|mins|minuto|minutos)\b/i);
    if (relativeMatch) {
        const amount = Number(relativeMatch[1]);
        const unit = relativeMatch[2].toLowerCase();
        if (!Number.isFinite(amount) || amount <= 0) {
            return null;
        }
        const delayMs = ['h', 'hr', 'hrs', 'hora', 'horas'].includes(unit)
            ? amount * 60 * 60 * 1000
            : amount * 60 * 1000;
        return {
            delayMs,
            executeAt: new Date(Date.now() + delayMs)
        };
    }

    const absoluteMatch = input.match(/^(?:a\s+las\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
    if (!absoluteMatch) {
        return null;
    }

    let hour = Number(absoluteMatch[1]);
    const minute = Number(absoluteMatch[2] || '0');
    const suffix = String(absoluteMatch[3] || '').toLowerCase();
    if (!Number.isFinite(hour) || !Number.isFinite(minute) || minute < 0 || minute > 59) {
        return null;
    }

    if (suffix) {
        if (hour < 1 || hour > 12) {
            return null;
        }
        if (suffix === 'am') {
            hour = hour === 12 ? 0 : hour;
        } else {
            hour = hour === 12 ? 12 : hour + 12;
        }
    } else if (hour < 0 || hour > 23) {
        return null;
    }

    const nowUtc = new Date();
    const mexicoNow = new Date(nowUtc.toLocaleString('en-US', { timeZone: 'America/Mexico_City' }));
    const mexicoTarget = new Date(mexicoNow);
    mexicoTarget.setSeconds(0, 0);
    mexicoTarget.setHours(hour, minute, 0, 0);
    if (mexicoTarget.getTime() <= mexicoNow.getTime()) {
        mexicoTarget.setDate(mexicoTarget.getDate() + 1);
    }
    const delayMs = mexicoTarget.getTime() - mexicoNow.getTime();
    return {
        delayMs,
        executeAt: new Date(nowUtc.getTime() + delayMs)
    };
}

function setScheduledGroupAction(groupJid, actionName, timer, executeAt) {
    const current = scheduledGroupActionsByGroup.get(groupJid) || {};
    current[actionName] = { timer, executeAt };
    scheduledGroupActionsByGroup.set(groupJid, current);
}

function clearScheduledGroupAction(groupJid, actionName) {
    const current = scheduledGroupActionsByGroup.get(groupJid);
    if (!current?.[actionName]) {
        return;
    }
    clearTimeout(current[actionName].timer);
    delete current[actionName];
    if (!current.close && !current.open) {
        scheduledGroupActionsByGroup.delete(groupJid);
        return;
    }
    scheduledGroupActionsByGroup.set(groupJid, current);
}

function getRulesText() {
    return [
        '✋Reglas del Grupo⚠️:',
        '',
        '🚫 Prohibido:',
        'Contenido sexual, erótico o +18 (incluye IA).',
        '',
        '🤝 Normas:',
        '* Respeto entre todos',
        '* Sin insultos ni acoso',
        '* No spam ni cadenas',
        '* 3 faltas acomuladas serán motivo de ban'
    ].join('\n');
}

function getUserCommandsText() {
    return [
        '🦫 🛠️ Comandos disponibles para todos:',
        '',
        '.sticker → crear sticker respondiendo a una imagen',
        '',
        '.reportar → reportar un mensaje respondiendo o citándolo',
        '',
        '.top → muestra el ranking de troncos del grupo',
        '',
        '.troncos → consulta cuántos troncos 🪵 tienes',
        '',
        '.random → menciona alguien al azar',
        '',
        '.comandos → muestra la lista de comandos para usuarios',
        '',
        '.dinamica → explica cómo ganar troncos 🪵',
        '',
        '.reglas → muestra las reglas del grupo',
        '',
        getCastorSignatureText()
    ].join('\n');
}

async function ensureLocalStorage() {
    if (isStorageReady) {
        return true;
    }
    ensureLocalStoreLoaded();
    isStorageReady = true;
    console.log(`Almacenamiento local activado en ${LOCAL_STORE_FILE}`);
    return true;
}

async function saveBotConfigRecord(update) {
    const store = ensureLocalStoreLoaded();
    store.botConfig = applyUpdateObject(store.botConfig || {}, update);
    saveLocalStore();
    return store.botConfig;
}

async function findModRecordsByUserIds(userIds) {
    const store = ensureLocalStoreLoaded();
    return userIds
        .map((userId) => ({ userId, ...(store.modRecords?.[userId] || {}) }))
        .filter((record) => Object.keys(record).length > 1);
}

async function upsertModRecord(userId, update) {
    const store = ensureLocalStoreLoaded();
    const current = store.modRecords?.[userId] || {
        userId,
        advertencias: 0,
        motivos: [],
        isBanned: false,
        intentosReingreso: 0,
        actividadMensajes: 0,
        ultimaActividad: null,
        countryOverride: '',
        flagOverride: '',
        troncos: 0
    };
    const next = applyUpdateObject(current, update);
    next.userId = userId;
    store.modRecords[userId] = next;
    saveLocalStore();
    return { ...next };
}

async function getModRecord(userId) {
    const store = ensureLocalStoreLoaded();
    return store.modRecords?.[userId] ? { ...store.modRecords[userId] } : null;
}

function getMessageReactionRecord(messageId) {
    const store = ensureLocalStoreLoaded();
    if (!store.messageReactions) {
        store.messageReactions = {};
    }
    return store.messageReactions[messageId] || null;
}

function upsertMessageReactionRecord(messageId, data) {
    const store = ensureLocalStoreLoaded();
    if (!store.messageReactions) {
        store.messageReactions = {};
    }
    store.messageReactions[messageId] = data;
    saveLocalStore();
}

function cleanOldReactionRecords() {
    const store = ensureLocalStoreLoaded();
    if (!store.messageReactions) return;
    const cutoff = Date.now() - (7 * 24 * 60 * 60 * 1000);
    let cleaned = 0;
    for (const [msgId, record] of Object.entries(store.messageReactions)) {
        if (record.createdAt && new Date(record.createdAt).getTime() < cutoff) {
            delete store.messageReactions[msgId];
            cleaned++;
        }
    }
    if (cleaned > 0) {
        saveLocalStore();
    }
}

function touchLastActivityAsync(userId) {
    if (!isStorageReady || !userId) {
        return;
    }
    upsertModRecord(userId, {
        $set: { ultimaActividad: new Date() },
        $inc: { actividadMensajes: 1 }
    }).catch(() => {});
}

function touchGroupActivity(groupJid) {
    if (PROACTIVE_GROUP_JIDS.length > 0 && PROACTIVE_GROUP_JIDS.includes(groupJid)) {
        lastGroupActivityAt = Date.now();
    }
}

function getProactiveState() {
    const store = ensureLocalStoreLoaded();
    return store.proactiveState || {};
}

function updateProactiveState(updates) {
    const store = ensureLocalStoreLoaded();
    store.proactiveState = { ...(store.proactiveState || {}), ...updates };
    saveLocalStore();
}

function isNightTime() {

    const mexicoNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Mexico_City' }));
    const hour = mexicoNow.getHours();
    if (PROACTIVE_NIGHT_START_HOUR > PROACTIVE_NIGHT_END_HOUR) {
        return hour >= PROACTIVE_NIGHT_START_HOUR || hour < PROACTIVE_NIGHT_END_HOUR;
    }
    return hour >= PROACTIVE_NIGHT_START_HOUR && hour < PROACTIVE_NIGHT_END_HOUR;
}

async function sendPrivateAdminMessage(sock, text) {
    const payload = typeof text === 'string'
        ? { text: sanitizeText(text, 9000) }
        : {
            ...(text || {}),
            text: typeof text?.text === 'string' ? sanitizeText(text.text, 9000) : text?.text
        };
    let lastError = null;
    const adminJids = await resolveAdminJids(sock);
    for (const adminJid of adminJids) {
        try {
            const result = await sock.sendMessage(adminJid, payload);
            console.log(`Mensaje privado al admin entregado a: ${adminJid}`);
            return result;
        } catch (error) {
            lastError = error;
        }
    }
    throw lastError || new Error('No pude entregar el mensaje al administrador.');
}

async function isGroupAdmin(sock, groupJid, userJid) {
    const metadata = await sock.groupMetadata(groupJid);
    const normalizedTarget = normalizePhoneForCompare(getNumberFromJid(userJid));
    const participant = metadata.participants.find((p) => {
        const current = normalizePhoneForCompare(getNumberFromJid(p.id));
        return current === normalizedTarget;
    });
    return participant?.admin === 'admin' || participant?.admin === 'superadmin';
}

async function getGroupDisplayName(sock, groupJid, userJid) {
    try {
        const metadata = await sock.groupMetadata(groupJid);
        const normalizedTarget = normalizePhoneForCompare(getNumberFromJid(userJid));
        const participant = metadata.participants.find((p) => {
            const current = normalizePhoneForCompare(getNumberFromJid(p.id));
            return current === normalizedTarget;
        });
        return sanitizeText(participant?.notify || participant?.name || getNumberFromJid(userJid) || userJid, 120);
    } catch (error) {
        return sanitizeText(getNumberFromJid(userJid) || userJid, 120);
    }
}

function getParticipantDisplayName(participant, fallbackJid = '') {
    const visibleName = sanitizeText(participant?.notify || participant?.name || participant?.pushName || '', 120);
    if (visibleName) {
        return visibleName;
    }
    const referenceJid = participant?.id || fallbackJid || '';
    const phone = sanitizeText(getNumberFromJid(referenceJid) || '', 120);
    if (phone) {
        return phone;
    }
    return 'Usuario sin nombre visible';
}

function getParticipantMentionLabel(participant, fallbackJid = '') {
    const visibleName = sanitizeText(participant?.notify || participant?.name || participant?.pushName || '', 40);
    if (visibleName) {
        return visibleName.replace(/\s+/g, '_');
    }
    const phone = sanitizeText(getNumberFromJid(participant?.id || fallbackJid || '') || '', 40);
    if (phone) {
        return phone;
    }
    return 'usuario';
}

function getModerationReferenceText(jid) {
    const rawJid = sanitizeText(jid || '', 160);
    const digits = sanitizeText(getNumberFromJid(jid) || '', 80);
    if (rawJid.endsWith('@lid')) {
        return digits ? `${digits} (ID interno de WhatsApp)` : rawJid;
    }
    return digits || rawJid;
}

function getReadableReportIdentity(info) {
    const label = sanitizeText(info?.mentionLabel || '', 60);
    const displayName = sanitizeText(info?.displayName || '', 120);
    if (label && displayName && label !== displayName) {
        return `@${label} (${displayName})`;
    }
    if (label) {
        return `@${label}`;
    }
    if (displayName) {
        return displayName;
    }
    return 'Usuario';
}

async function getGroupParticipantSummary(sock, groupJid, userJid) {
    try {
        const metadata = await sock.groupMetadata(groupJid);
        const normalizedTarget = normalizePhoneForCompare(getNumberFromJid(userJid));
        const participant = metadata.participants.find((p) => {
            const current = normalizePhoneForCompare(getNumberFromJid(p.id));
            return current === normalizedTarget;
        }) || null;
        return {
            groupName: sanitizeText(metadata.subject || groupJid, 160),
            displayName: getParticipantDisplayName(participant, userJid),
            mentionLabel: getParticipantMentionLabel(participant, userJid),
            moderationReference: getModerationReferenceText(userJid)
        };
    } catch (error) {
        return {
            groupName: sanitizeText(groupJid, 160),
            displayName: sanitizeText(getNumberFromJid(userJid) || userJid, 120),
            mentionLabel: sanitizeText(getNumberFromJid(userJid) || 'usuario', 40),
            moderationReference: getModerationReferenceText(userJid)
        };
    }
}

async function senderIsAuthorizedAdmin(sock, msg, remoteJid) {
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const senderNumber = getNumberFromJid(senderJid);
    if (isOwnerByNumber(senderNumber)) {
        return true;
    }
    if (!remoteJid.endsWith('@g.us')) {
        return false;
    }
    return isGroupAdmin(sock, remoteJid, senderJid);
}

function extractCandidateNumber(value) {
    const raw = String(value || '');
    if (!raw) {
        return '';
    }
    if (raw.includes('@')) {
        return getNumberFromJid(raw);
    }
    return cleanDigits(raw);
}

function isLikelyPhoneNumber(number) {
    const digits = cleanDigits(number);
    return digits.length >= 10 && digits.length <= 15;
}

async function resolveCountryAndFlag(sock, groupJid, participantJid) {
    if (isStorageReady) {
        const overrideRecord = await getModRecord(participantJid);
        if (overrideRecord?.countryOverride) {
            return {
                country: overrideRecord.countryOverride,
                flag: overrideRecord.flagOverride || '🌍'
            };
        }
    }

    const number = getNumberFromJid(participantJid);
    const jidDomain = getDomainFromJid(participantJid);
    if (jidDomain === 's.whatsapp.net') {
        return {
            country: getCountryFromNumber(number),
            flag: getFlagFromNumber(number)
        };
    }

    try {
        const metadata = await sock.groupMetadata(groupJid);
        const participants = metadata?.participants || [];
        const target = participants.find((p) => p.id === participantJid || p.lid === participantJid);
        if (target) {
            const candidates = [
                { key: 'id', value: target.id },
                { key: 'phoneNumber', value: target.phoneNumber },
                { key: 'pn', value: target.pn },
                { key: 'jid', value: target.jid },
                { key: 'lid', value: target.lid }
            ];
            for (const candidate of candidates) {
                const raw = String(candidate.value || '');
                if (!raw) {
                    continue;
                }
                const domain = getDomainFromJid(raw);
                const isPhoneJidCandidate = domain === 's.whatsapp.net';
                const isPhoneFieldCandidate = (candidate.key === 'phoneNumber' || candidate.key === 'pn') && isLikelyPhoneNumber(raw);
                if (!isPhoneJidCandidate && !isPhoneFieldCandidate) {
                    continue;
                }
                const candidateNumber = extractCandidateNumber(raw);
                if (isLikelyPhoneNumber(candidateNumber)) {
                    return {
                        country: getCountryFromNumber(candidateNumber),
                        flag: getFlagFromNumber(candidateNumber)
                    };
                }
            }
        }
    } catch (error) {
    }

    return {
        country: 'un país no identificado',
        flag: '🌍'
    };
}

async function sendWelcome(sock, groupJid, participantJid) {
    const number = getNumberFromJid(participantJid);
    const mention = number ? `@${number}` : '@usuario';
    const resolvedLocation = await resolveCountryAndFlag(sock, groupJid, participantJid);
    const country = resolvedLocation.country;
    const flag = resolvedLocation.flag;
    const welcomeAndRulesText = [
        `${CASTOR_EMOJI} ¡Un nuevo castor ha llegado al estanque! Bienvenido/a ${mention}`,
        `Nos saludas desde ${flag} ${country}. Soy Castor Bot, el guardián de este dique. ¡Ponte cómodo y ayudemos a construir!`,
        '',
        getRulesText(),
        '',
        '🔗 Link del grupo: https://chat.whatsapp.com/IIiwVeYGLUV8gbGXU4SfEz'
    ].join('\n');
    const commandsInfoText = getUserCommandsText();

    let profileUrl = null;
    try {
        profileUrl = await sock.profilePictureUrl(participantJid, 'image');
    } catch (error) {
        profileUrl = null;
    }

    const imageUrl = profileUrl || CASTOR_DEFAULT_IMAGE_URL;

    try {
        await sock.sendMessage(groupJid, {
            image: { url: imageUrl },
            caption: welcomeAndRulesText,
            mentions: number ? [participantJid] : []
        });
        if (!SAFE_COMPACT_WELCOME) {
            await sock.sendMessage(groupJid, {
                text: commandsInfoText
            });
        }
    } catch (error) {
        await sock.sendMessage(groupJid, {
            text: welcomeAndRulesText,
            mentions: number ? [participantJid] : []
        });
        if (!SAFE_COMPACT_WELCOME) {
            await sock.sendMessage(groupJid, {
                text: commandsInfoText
            });
        }
    }
}

async function sendBatchedWelcome(sock, groupJid, participantJids) {
    const welcomeMentions = [];
    const mentionsArray = [];

    for (const jid of participantJids) {
        const number = getNumberFromJid(jid);
        if (number) {
            welcomeMentions.push(`@${number}`);
            mentionsArray.push(jid);
        }
    }

    if (welcomeMentions.length === 0) {
        return;
    }

    const isMultiple = welcomeMentions.length > 1;
    const title = isMultiple
        ? `${CASTOR_EMOJI} ¡Nuevos castores han llegado al estanque! Bienvenidos:`
        : `${CASTOR_EMOJI} ¡Un nuevo castor ha llegado al estanque! Bienvenido/a:`;
    let locationLine = 'Soy Castor Bot, el guardián de este dique. ¡Pónganse cómodos y ayudemos a construir!';
    if (!isMultiple && mentionsArray[0]) {
        const resolvedLocation = await resolveCountryAndFlag(sock, groupJid, mentionsArray[0]);
        locationLine = `Nos saludas desde ${resolvedLocation.flag} ${resolvedLocation.country}. Soy Castor Bot, el guardián de este dique. ¡Ponte cómodo y ayudemos a construir!`;
    }

    const welcomeAndRulesText = [
        title,
        welcomeMentions.join(', '),
        '',
        locationLine,
        '',
        getRulesText(),
        '',
        '🔗 Link del grupo: https://chat.whatsapp.com/IIiwVeYGLUV8gbGXU4SfEz'
    ].join('\n');

    const imageUrl = CASTOR_DEFAULT_IMAGE_URL;

    try {
        await sock.sendMessage(groupJid, {
            image: { url: imageUrl },
            caption: welcomeAndRulesText,
            mentions: mentionsArray
        });

        if (!SAFE_COMPACT_WELCOME) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            await sock.sendMessage(groupJid, { text: getUserCommandsText() });
        }
    } catch (error) {
        await sock.sendMessage(groupJid, {
            text: welcomeAndRulesText,
            mentions: mentionsArray
        });
    }
}

async function handleReportCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'Este comando solo funciona en grupos.' }, { quoted: msg });
        return;
    }
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const senderKey = normalizePhoneForCompare(getNumberFromJid(senderJid));
    const now = Date.now();
    const last = reportCooldownByUser.get(senderKey) || 0;
    if (now - last < 60000) {
        const wait = Math.ceil((60000 - (now - last)) / 1000);
        await sock.sendMessage(remoteJid, { text: `Debes esperar ${wait}s para usar .reportar de nuevo.` }, { quoted: msg });
        return;
    }

    const quoted = getQuotedPayload(msg.message);
    if (!quoted?.quotedParticipant || !quoted?.quotedMessage) {
        await sock.sendMessage(remoteJid, { text: 'Para usar .reportar debes responder a un mensaje.' }, { quoted: msg });
        return;
    }

    if (await isGroupAdmin(sock, remoteJid, quoted.quotedParticipant)) {
        await sock.sendMessage(remoteJid, { text: 'No puedes reportar a un administrador del grupo.' }, { quoted: msg });
        return;
    }

    reportCooldownByUser.set(senderKey, now);
    const detail = describeQuotedContent(quoted.quotedMessage);
    const reporterId = msg.key.participant || msg.key.remoteJid;
    const offenderId = quoted.quotedParticipant;
    const reporterInfo = await getGroupParticipantSummary(sock, remoteJid, reporterId);
    const offenderInfo = await getGroupParticipantSummary(sock, remoteJid, offenderId);
    const motive = sanitizeText(text).split(/\s+/).slice(1).join(' ');
    const cleanMotive = motive || 'Sin motivo adicional.';
    const reporterReadable = getReadableReportIdentity(reporterInfo);
    const offenderReadable = getReadableReportIdentity(offenderInfo);

    const reportText = [
        '🧾 REPORTE FORENSE',
        `Grupo: ${reporterInfo.groupName}`,
        `Reportante: ${reporterReadable}`,
        `Referencia reportante: ${reporterInfo.moderationReference}`,
        `ID reportante: ${reporterId}`,
        `Infractor: ${offenderReadable}`,
        `Referencia infractor: ${offenderInfo.moderationReference}`,
        `ID infractor: ${offenderId}`,
        `Motivo: ${cleanMotive}`,
        `Tipo de contenido: ${detail.mediaType}`,
        `Texto citado: ${detail.text || '(sin texto visible)'}`,
        `Contenido crudo: ${detail.raw || '(sin contenido)'}`,
        '',
        'Acciones rápidas:',
        `.advertir ${offenderId}`,
        `.ban ${offenderId}`
    ].join('\n');

    try {
        const sentReport = await sendPrivateAdminMessage(sock, {
            text: reportText,
            mentions: [reporterId, offenderId]
        });
        reportReferenceMap.set(sentReport.key.id, { offenderJid: offenderId, groupJid: remoteJid });
        await sendPrivateAdminMessage(sock, `.advertir ${offenderId}`);
        await sock.sendMessage(remoteJid, { text: '✅ Reporte recibido. Evidencia preservada y enviada a administración.' }, { quoted: msg });
        await sendReactionSticker(sock, remoteJid, 'reportar.webp');
    } catch (error) {
        console.error('No pude entregar el reporte al administrador:', error?.message || error);
        await sock.sendMessage(remoteJid, { text: '⚠️ Recibí el reporte, pero no pude enviarlo a administración. En tu chat privado con el bot usa primero .miid y luego .setadmin para vincular correctamente tu ID admin.' }, { quoted: msg });
    }
}

async function resolveTargetForModeration(msg, text) {
    const fromTextOrMention = parseTargetFromTextOrMention(msg, text);
    if (fromTextOrMention) {
        return { targetJid: fromTextOrMention, source: 'directo', groupFromReport: null };
    }

    const quoted = getQuotedPayload(msg.message);
    if (!quoted) {
        return { targetJid: null, source: null, groupFromReport: null };
    }

    const messageText = extractTextFromMessage(msg.message);
    if (messageText.toLowerCase().startsWith('.advertir') || messageText.toLowerCase().startsWith('.unban')) {
        if (quoted.quotedStanzaId && reportReferenceMap.has(quoted.quotedStanzaId)) {
            const data = reportReferenceMap.get(quoted.quotedStanzaId);
            return { targetJid: data.offenderJid, source: 'reporte', groupFromReport: data.groupJid };
        }
        const quotedText = extractTextFromMessage(quoted.quotedMessage || {});
        const parsedTarget = parseTargetFromReportText(quotedText);
        const parsedGroup = parseGroupFromReportText(quotedText);
        if (parsedTarget) {
            return { targetJid: parsedTarget, source: 'reporte', groupFromReport: parsedGroup };
        }
    }

    if (quoted.quotedParticipant) {
        return { targetJid: quoted.quotedParticipant, source: 'respuesta', groupFromReport: null };
    }
    return { targetJid: null, source: null, groupFromReport: null };
}

async function handleWarnCommand(sock, msg, text, remoteJid) {
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: almacenamiento local no está listo.' }, { quoted: msg });
        return;
    }

    const resolved = await resolveTargetForModeration(msg, text);
    if (!resolved.targetJid) {
        await sock.sendMessage(remoteJid, { text: 'Usa .advertir con mención, ID o respondiendo al reporte.' }, { quoted: msg });
        return;
    }

    const currentGroup = remoteJid.endsWith('@g.us') ? remoteJid : resolved.groupFromReport;
    if (currentGroup && await isGroupAdmin(sock, currentGroup, resolved.targetJid)) {
        await sock.sendMessage(remoteJid, { text: 'No puedes advertir a un administrador del grupo.' }, { quoted: msg });
        return;
    }

    const senderJid = msg.key.participant || msg.key.remoteJid;
    const reason = `Advertencia por ${senderJid} en ${new Date().toISOString()}`;
    const mention = `@${getNumberFromJid(resolved.targetJid)}`;
    const result = await applyWarning(sock, resolved.targetJid, currentGroup, reason);
    await sendCastorSealSticker(sock, remoteJid, msg);

    if (result.warningCount < 3) {
        await sock.sendMessage(remoteJid, {
            text: `⚠️ ${mention}, has sido advertido ([${result.warningCount}]/3). Se ha registrado la evidencia.`,
            mentions: [resolved.targetJid]
        }, { quoted: msg });
        return;
    }

    const resultText = result.kicked
        ? `⛔ ${mention} alcanzó 3/3 advertencias y fue expulsado automáticamente.`
        : result.kickPermissionDenied
            ? `⛔ ${mention} alcanzó 3/3 advertencias y quedó marcado como baneado. Necesito ser administrador del grupo para expulsarlo automáticamente.`
            : `⛔ ${mention} alcanzó 3/3 advertencias y quedó marcado como baneado. No pude expulsarlo automáticamente.`;
    await sock.sendMessage(remoteJid, { text: resultText, mentions: [resolved.targetJid] }, { quoted: msg });
    await sendReactionSticker(sock, remoteJid, 'advertir.webp');
}

async function handleUnbanCommand(sock, msg, text, remoteJid) {
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: almacenamiento local no está listo.' }, { quoted: msg });
        return;
    }

    const resolved = await resolveTargetForModeration(msg, text);
    if (!resolved.targetJid) {
        await sock.sendMessage(remoteJid, { text: 'Usa .unban con mención, ID o respondiendo al reporte.' }, { quoted: msg });
        return;
    }

    await upsertModRecord(resolved.targetJid, {
        $set: { isBanned: false, advertencias: 0 }
    });

    const mention = `@${getNumberFromJid(resolved.targetJid)}`;
    await sock.sendMessage(remoteJid, {
        text: `✅ ${mention} fue desbaneado y su contador de advertencias se reinició.`,
        mentions: [resolved.targetJid]
    }, { quoted: msg });
}

async function handleBanCommand(sock, msg, text, remoteJid) {
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: almacenamiento local no está listo.' }, { quoted: msg });
        return;
    }

    const resolved = await resolveTargetForModeration(msg, text);
    if (!resolved.targetJid) {
        await sock.sendMessage(remoteJid, { text: 'Usa .ban con mención, ID o respondiendo al reporte.' }, { quoted: msg });
        return;
    }

    const currentGroup = remoteJid.endsWith('@g.us') ? remoteJid : resolved.groupFromReport;
    if (!currentGroup) {
        await sock.sendMessage(remoteJid, { text: 'Para usar .ban necesito conocer el grupo de origen.' }, { quoted: msg });
        return;
    }
    if (await isGroupAdmin(sock, currentGroup, resolved.targetJid)) {
        await sock.sendMessage(remoteJid, { text: 'No puedes banear a un administrador del grupo.' }, { quoted: msg });
        return;
    }

    await upsertModRecord(resolved.targetJid, { $set: { isBanned: true } });

    let removed = false;
    try {
        await sock.groupParticipantsUpdate(currentGroup, [resolved.targetJid], 'remove');
        removed = true;
    } catch (error) {
        const failureText = isGroupPermissionError(error)
            ? 'Necesito ser administrador del grupo para banear usuarios.'
            : 'No pude completar el ban en este momento. Intenta de nuevo.';
        await sock.sendMessage(remoteJid, { text: failureText }, { quoted: msg });
        return;
    }

    const mention = `@${getNumberFromJid(resolved.targetJid) || 'usuario'}`;
    const resultText = removed
        ? `⛔ ${mention} fue baneado y eliminado del grupo.`
        : `⛔ ${mention} quedó marcado como baneado, pero no pude eliminarlo del grupo.`;
    await sock.sendMessage(remoteJid, {
        text: resultText,
        mentions: [resolved.targetJid]
    }, { quoted: msg });
}

async function handleStickerCommand(sock, msg, remoteJid) {
    const quoted = getQuotedPayload(msg.message);
    if (!quoted?.quotedMessage) {
        await sock.sendMessage(remoteJid, { text: 'Para usar .sticker debes responder a una imagen.' }, { quoted: msg });
        return;
    }

    const quotedBody = getMainMessageObject(quoted.quotedMessage);
    const imageMessage = quotedBody?.imageMessage;
    if (!imageMessage) {
        await sock.sendMessage(remoteJid, { text: 'El mensaje citado no contiene una imagen válida.' }, { quoted: msg });
        return;
    }

    try {
        const stream = await downloadContentFromMessage(imageMessage, 'image');
        const imageBuffer = await streamToBuffer(stream);
        await sendCastorSealSticker(sock, remoteJid, msg);
        await sock.sendMessage(remoteJid, { sticker: imageBuffer }, { quoted: msg });
        await sock.sendMessage(remoteJid, { text: '¡Misión Dique Cumplida! Tu imagen ya es sticker.' }, { quoted: msg });
    } catch (error) {
        await sock.sendMessage(remoteJid, { text: 'No pude convertir esa imagen a sticker.' }, { quoted: msg });
    }
}

async function sendCastorSealSticker(sock, remoteJid, quotedMsg) {
    if (!CASTOR_SEAL_STICKER_URL || SAFE_DISABLE_SEAL_STICKER) {
        return;
    }
    try {
        await sock.sendMessage(remoteJid, { sticker: { url: CASTOR_SEAL_STICKER_URL } }, quotedMsg ? { quoted: quotedMsg } : undefined);
    } catch (error) {
    }
}

async function sendReactionSticker(sock, remoteJid, stickerFileName) {
    const stickerPath = path.join(process.cwd(), 'stickers', stickerFileName);
    if (!fs.existsSync(stickerPath)) {
        console.log(`⚠️ No se encontró el sticker: ${stickerPath}`);
        return;
    }
    await new Promise((resolve) => setTimeout(resolve, 2500));
    await sock.sendMessage(remoteJid, { sticker: fs.readFileSync(stickerPath) });
}

async function applyWarning(sock, targetJid, groupJid, reason) {
    const record = await upsertModRecord(targetJid, {
        $inc: { advertencias: 1 },
        $push: { motivos: sanitizeText(reason, 500) }
    });
    const warningCount = record.advertencias;
    let kicked = false;
    let kickPermissionDenied = false;
    if (warningCount >= 3) {
        await upsertModRecord(targetJid, { $set: { isBanned: true } });
        if (groupJid && !SAFE_DISABLE_AUTO_KICK) {
            try {
                await sock.groupParticipantsUpdate(groupJid, [targetJid], 'remove');
                kicked = true;
            } catch (error) {
                kicked = false;
                kickPermissionDenied = isGroupPermissionError(error);
            }
        }
    }
    return { warningCount, kicked, kickPermissionDenied };
}

async function handleGhostsCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .fantasmas solo funciona en grupos.' }, { quoted: msg });
        return;
    }
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: almacenamiento local no está listo.' }, { quoted: msg });
        return;
    }

    const argDays = Number(sanitizeText(text).split(/\s+/)[1] || '15');
    const days = Number.isFinite(argDays) && argDays > 0 ? Math.floor(argDays) : 15;
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const metadata = await sock.groupMetadata(remoteJid);
    const participants = metadata.participants || [];
    const participantIds = participants.map((p) => p.id);
    const records = await findModRecordsByUserIds(participantIds);

    const recordsByUser = new Map(records.map((item) => [item.userId, item]));
    const inactive = [];
    for (const participant of participants) {
        const isAdmin = participant.admin === 'admin' || participant.admin === 'superadmin';
        if (isAdmin) {
            continue;
        }
        const rec = recordsByUser.get(participant.id);
        if (!rec?.ultimaActividad || new Date(rec.ultimaActividad) < cutoff) {
            const name = getParticipantDisplayName(participant, participant.id);
            const lastActivityText = rec?.ultimaActividad
                ? new Date(rec.ultimaActividad).toISOString().slice(0, 10)
                : 'sin registro';
            const moderationId = sanitizeText(participant.id, 160);
            inactive.push(`• ${name} - última actividad: ${lastActivityText}\n  ID moderación: ${moderationId}`);
        }
    }

    const senderJid = msg.key.participant || msg.key.remoteJid;
    const summary = inactive.length > 0
        ? inactive.join('\n')
        : 'No se detectaron usuarios inactivos en ese rango.';
    const privateText = `👻 Reporte de fantasmas\nGrupo: ${metadata.subject}\nRango: ${days} días\nTotal inactivos: ${inactive.length}\n\n${summary}`;
    await sock.sendMessage(senderJid, { text: privateText });
    await sock.sendMessage(remoteJid, { text: '👻 Lista de inactivos enviada por privado al administrador.' }, { quoted: msg });
}

async function handleTopCommand(sock, msg, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .top solo funciona en grupos.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: '⚠️ El ranking de actividad necesita almacenamiento local activo.' }, { quoted: msg });
        return;
    }

    const metadata = await sock.groupMetadata(remoteJid);
    const participants = (metadata.participants || []).filter((participant) => participant.id !== sock.user?.id);
    const participantIds = participants.map((participant) => participant.id);
    const records = await findModRecordsByUserIds(participantIds);

    const participantsById = new Map(participants.map((participant) => [participant.id, participant]));
    const topEntries = records
        .map((record) => {
            const participant = participantsById.get(record.userId);
            if (!participant) {
                return null;
            }
            const displayName = getParticipantDisplayName(participant, record.userId);
            const mentionLabel = getParticipantMentionLabel(participant, record.userId);
            return {
                userId: record.userId,
                displayName,
                mentionLabel,
                troncos: Number(record.troncos || 0),
                actividadMensajes: Number(record.actividadMensajes || 0),
                ultimaActividad: record.ultimaActividad ? new Date(record.ultimaActividad).getTime() : 0
            };
        })
        .filter((entry) => entry && (entry.troncos > 0 || entry.actividadMensajes > 0))
        .sort((a, b) => {
            if (b.troncos !== a.troncos) {
                return b.troncos - a.troncos;
            }
            return b.actividadMensajes - a.actividadMensajes;
        })
        .slice(0, 10);

    if (!topEntries.length) {
        await sock.sendMessage(remoteJid, { text: '🪵 Aún no hay troncos ni actividad registrada para mostrar el ranking.' }, { quoted: msg });
        return;
    }

    const lines = topEntries.map((entry, index) => `${index + 1}. @${entry.mentionLabel} (${entry.displayName}) — 🪵 ${entry.troncos} tronco${entry.troncos !== 1 ? 's' : ''} | ${entry.actividadMensajes} msgs`);
    await sock.sendMessage(remoteJid, {
        text: `🪵 Ranking de Troncos del Estanque\n\n${lines.join('\n')}`,
        mentions: topEntries.map((entry) => entry.userId)
    }, { quoted: msg });
}

async function handleRandomCommand(sock, msg, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .random solo funciona en grupos.' }, { quoted: msg });
        return;
    }

    const metadata = await sock.groupMetadata(remoteJid);
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const candidates = (metadata.participants || [])
        .map((participant) => participant.id)
        .filter((participantId) => participantId !== sock.user?.id && participantId !== senderJid);

    if (!candidates.length) {
        await sock.sendMessage(remoteJid, { text: '🎲 No encontré suficientes usuarios para elegir al azar.' }, { quoted: msg });
        return;
    }

    const selectedJid = candidates[Math.floor(Math.random() * candidates.length)];
    const selectedParticipant = (metadata.participants || []).find((participant) => participant.id === selectedJid) || null;
    const displayName = getParticipantDisplayName(selectedParticipant, selectedJid);
    const mentionLabel = getParticipantMentionLabel(selectedParticipant, selectedJid);
    await sock.sendMessage(remoteJid, {
        text: `🎲 El castor eligió a @${mentionLabel} (${displayName})`,
        mentions: [selectedJid]
    }, { quoted: msg });
}

function isGroupPermissionError(error) {
    const message = String(error?.message || '').toLowerCase();
    const statusCode = Number(error?.output?.statusCode || error?.data?.statusCode || 0);
    return statusCode === 401
        || statusCode === 403
        || /not-authorized|not authorized|forbidden|admin|permission|allow only admins/i.test(message);
}

async function handleCloseGroupCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .cerrar solo funciona en grupos.' }, { quoted: msg });
        return;
    }

    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }

    const rawDuration = sanitizeText(text).replace(/^\.cerrar\s*/i, '').trim();
    if (rawDuration) {
        const scheduledClose = parseFutureActionTime(rawDuration);
        if (scheduledClose) {
            clearScheduledGroupAction(remoteJid, 'close');
            const executeAtLabel = formatMexicoDateTime(scheduledClose.executeAt);
            const timer = setTimeout(async () => {
                try {
                    await sock.groupSettingUpdate(remoteJid, 'announcement');
                    await sock.sendMessage(remoteJid, { text: '🔒 El grupo se cerró automáticamente. Solo administradores pueden enviar mensajes.' });
                    await sendReactionSticker(sock, remoteJid, 'cerrar.webp');
                } catch (error) {
                } finally {
                    clearScheduledGroupAction(remoteJid, 'close');
                }
            }, scheduledClose.delayMs);
            setScheduledGroupAction(remoteJid, 'close', timer, scheduledClose.executeAt);
            await sock.sendMessage(remoteJid, { text: `⏳ Grupo programado para cerrarse el ${executeAtLabel}.` }, { quoted: msg });
            return;
        }

        const durationMs = parseCloseDurationMs(rawDuration);
        if (!durationMs) {
            await sock.sendMessage(remoteJid, { text: 'Formato inválido. Usa por ejemplo: .cerrar 20 min, .cerrar en 15 min o .cerrar 10 pm' }, { quoted: msg });
            return;
        }

        clearScheduledGroupAction(remoteJid, 'close');
        const activeTimer = closeTimersByGroup.get(remoteJid);
        if (activeTimer?.timer) {
            clearTimeout(activeTimer.timer);
        }

        try {
            await sock.groupSettingUpdate(remoteJid, 'announcement');
        } catch (error) {
            const failureText = isGroupPermissionError(error)
                ? 'Necesito ser administrador del grupo para cerrarlo.'
                : 'No pude cerrar el grupo en este momento. Intenta de nuevo.';
            await sock.sendMessage(remoteJid, { text: failureText }, { quoted: msg });
            return;
        }
        const reopenAt = new Date(Date.now() + durationMs);
        const reopenAtLabel = formatMexicoDateTime(reopenAt);
        await sock.sendMessage(remoteJid, { text: `🔒 Grupo cerrado por ${rawDuration}. Se abrirá automáticamente el ${reopenAtLabel}.` }, { quoted: msg });
        await sendReactionSticker(sock, remoteJid, 'cerrar.webp');

        const timer = setTimeout(async () => {
            try {
                await sock.groupSettingUpdate(remoteJid, 'not_announcement');
                await sock.sendMessage(remoteJid, { text: '🔓 El grupo se abrió automáticamente. Ya todos pueden enviar mensajes.' });
                await sendReactionSticker(sock, remoteJid, 'abrir.webp');
            } catch (error) {
            } finally {
                closeTimersByGroup.delete(remoteJid);
            }
        }, durationMs);

        closeTimersByGroup.set(remoteJid, { timer, reopenAt });
        return;
    }

    const activeTimer = closeTimersByGroup.get(remoteJid);
    if (activeTimer?.timer) {
        clearTimeout(activeTimer.timer);
        closeTimersByGroup.delete(remoteJid);
    }
    clearScheduledGroupAction(remoteJid, 'close');

    try {
        await sock.groupSettingUpdate(remoteJid, 'announcement');
    } catch (error) {
        const failureText = isGroupPermissionError(error)
            ? 'Necesito ser administrador del grupo para cerrarlo.'
            : 'No pude cerrar el grupo en este momento. Intenta de nuevo.';
        await sock.sendMessage(remoteJid, { text: failureText }, { quoted: msg });
        return;
    }
    await sock.sendMessage(remoteJid, { text: '🔒 Grupo cerrado hasta nuevo aviso. Solo administradores pueden enviar mensajes.' }, { quoted: msg });
    await sendReactionSticker(sock, remoteJid, 'cerrar.webp');
}

async function handleOpenGroupCommand(sock, msg, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .abrir solo funciona en grupos.' }, { quoted: msg });
        return;
    }

    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }

    const rawSchedule = extractTextFromMessage(msg.message).replace(/^\.abrir\s*/i, '').trim();
    if (rawSchedule) {
        const scheduledOpen = parseFutureActionTime(rawSchedule);
        if (!scheduledOpen) {
            await sock.sendMessage(remoteJid, { text: 'Formato inválido. Usa por ejemplo: .abrir en 2 min o .abrir 7 am' }, { quoted: msg });
            return;
        }
        clearScheduledGroupAction(remoteJid, 'open');
        const activeCloseTimer = closeTimersByGroup.get(remoteJid);
        if (activeCloseTimer?.timer) {
            clearTimeout(activeCloseTimer.timer);
            closeTimersByGroup.delete(remoteJid);
        }
        const executeAtLabel = formatMexicoDateTime(scheduledOpen.executeAt);
        const timer = setTimeout(async () => {
            try {
                await sock.groupSettingUpdate(remoteJid, 'not_announcement');
                await sock.sendMessage(remoteJid, { text: '🔓 El grupo se abrió automáticamente. Ya todos pueden enviar mensajes.' });
                await sendReactionSticker(sock, remoteJid, 'abrir.webp');
            } catch (error) {
            } finally {
                clearScheduledGroupAction(remoteJid, 'open');
            }
        }, scheduledOpen.delayMs);
        setScheduledGroupAction(remoteJid, 'open', timer, scheduledOpen.executeAt);
        await sock.sendMessage(remoteJid, { text: `⏳ Grupo programado para abrirse el ${executeAtLabel}.` }, { quoted: msg });
        return;
    }

    const activeTimer = closeTimersByGroup.get(remoteJid);
    if (activeTimer?.timer) {
        clearTimeout(activeTimer.timer);
        closeTimersByGroup.delete(remoteJid);
    }
    clearScheduledGroupAction(remoteJid, 'open');

    try {
        await sock.groupSettingUpdate(remoteJid, 'not_announcement');
    } catch (error) {
        const failureText = isGroupPermissionError(error)
            ? 'Necesito ser administrador del grupo para abrirlo.'
            : 'No pude abrir el grupo en este momento. Intenta de nuevo.';
        await sock.sendMessage(remoteJid, { text: failureText }, { quoted: msg });
        return;
    }
    await sock.sendMessage(remoteJid, { text: '🔓 Grupo abierto. Todos los miembros ya pueden enviar mensajes.' }, { quoted: msg });
    await sendReactionSticker(sock, remoteJid, 'abrir.webp');
}

async function handlePingCommand(sock, msg, remoteJid) {
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    await sock.sendMessage(remoteJid, { text: '✅ Castor Bot activo.' }, { quoted: msg });
}

async function handleCommandsListCommand(sock, msg, remoteJid) {
    await sock.sendMessage(remoteJid, { text: getUserCommandsText() }, { quoted: msg });
}

async function handleRulesCommand(sock, msg, remoteJid) {
    await sock.sendMessage(remoteJid, { text: getRulesText() }, { quoted: msg });
}

async function handleMyIdCommand(sock, msg, remoteJid) {
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const senderNumber = getNumberFromJid(senderJid);
    const privateJid = msg.key.remoteJid || '';
    const canLinkAdmin = await canManageAdminLink(sock, msg, remoteJid);
    const details = [
        '🪪 Identificadores detectados',
        `Chat actual: ${privateJid || '(sin dato)'}`,
        `Sender: ${senderJid || '(sin dato)'}`,
        `Número detectado: ${senderNumber || '(sin dato)'}`,
        `Tipo de chat: ${privateJid.endsWith('@g.us') ? 'grupo' : 'privado'}`,
        `Permiso para vincular: ${canLinkAdmin ? 'sí' : 'no'}`,
        '',
        canLinkAdmin
            ? (privateJid.endsWith('@g.us')
                ? 'Puedes usar .setadmin aquí para vincular tu ID actual, o en privado para vincular tu chat privado.'
                : 'Si este es tu chat privado correcto, usa .setadmin aquí mismo.')
            : 'Si este no te reconoce como admin, envía este mensaje al desarrollador para ajustar el vínculo.'
    ].join('\n');
    await sock.sendMessage(remoteJid, { text: details }, { quoted: msg });
}

async function handleSetAdminCommand(sock, msg, remoteJid) {
    const senderJid = msg.key.participant || msg.key.remoteJid;
    if (!(await canManageAdminLink(sock, msg, remoteJid))) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo el admin principal.' }, { quoted: msg });
        return;
    }
    const privateJid = msg.key.remoteJid || '';
    if (!privateJid) {
        await sock.sendMessage(remoteJid, { text: 'No pude detectar el chat actual para vincularlo.' }, { quoted: msg });
        return;
    }
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: 'No pude guardar el chat admin porque el almacenamiento local no está listo.' }, { quoted: msg });
        return;
    }
    await saveAdminPrivateJids(privateJid.endsWith('@g.us') ? '' : privateJid, senderJid);
    await sock.sendMessage(remoteJid, {
        text: `✅ Admin vinculado.\nChat: ${privateJid}\nSender: ${senderJid}`
    }, { quoted: msg });
}

async function handleReactionForTroncos(sock, msg) {
    if (!isStorageReady) return;
    const reaction = msg.message?.reactionMessage;
    if (!reaction) return;
    const emoji = reaction.text || '';
    const reactedMessageKey = reaction.key;
    if (!reactedMessageKey?.id) return;
    const remoteJid = msg.key.remoteJid;
    if (!remoteJid || !remoteJid.endsWith('@g.us')) return;
    if (!emoji || !POSITIVE_REACTION_EMOJIS.has(emoji)) return;

    const reactorJid = msg.key.participant || msg.key.remoteJid;
    if (!reactorJid) return;
    const authorJid = reactedMessageKey.participant || '';
    if (!authorJid) return;

    if (normalizePhoneForCompare(getNumberFromJid(reactorJid)) === normalizePhoneForCompare(getNumberFromJid(authorJid))) return;

    const messageId = reactedMessageKey.id;
    let record = getMessageReactionRecord(messageId);
    if (!record) {
        record = {
            authorJid,
            groupJid: remoteJid,
            reactors: [],
            milestone5: false,
            milestone10: false,
            createdAt: new Date().toISOString()
        };
    }

    const reactorNormalized = normalizePhoneForCompare(getNumberFromJid(reactorJid));
    const alreadyReacted = record.reactors.some(r =>
        normalizePhoneForCompare(getNumberFromJid(r)) === reactorNormalized
    );
    if (alreadyReacted) return;

    record.reactors.push(reactorJid);
    const uniqueCount = record.reactors.length;

    if (uniqueCount >= 5 && !record.milestone5) {
        record.milestone5 = true;
        await upsertModRecord(authorJid, { $inc: { troncos: 1 } });
        const authorNumber = getNumberFromJid(authorJid);
        const mention = authorNumber ? `@${authorNumber}` : 'alguien';
        await sock.sendMessage(remoteJid, {
            text: `\ud83e\udeb5 \u00a1${mention} gan\u00f3 1 tronco! Su mensaje alcanz\u00f3 5 reacciones. \u00a1Sigue construyendo el dique!`,
            mentions: authorJid ? [authorJid] : []
        });
    }

    if (uniqueCount >= 10 && !record.milestone10) {
        record.milestone10 = true;
        await upsertModRecord(authorJid, { $inc: { troncos: 1 } });
        const authorNumber = getNumberFromJid(authorJid);
        const mention = authorNumber ? `@${authorNumber}` : 'alguien';
        await sock.sendMessage(remoteJid, {
            text: `\ud83e\udeb5\ud83e\udeb5 \u00a1${mention} gan\u00f3 1 tronco extra! Su mensaje alcanz\u00f3 10 reacciones. \u00a1Eres la estrella del estanque!`,
            mentions: authorJid ? [authorJid] : []
        });
    }

    upsertMessageReactionRecord(messageId, record);
}

async function handleTroncosCommand(sock, msg, remoteJid) {
    if (!isStorageReady) {
        await sock.sendMessage(remoteJid, { text: '\u26a0\ufe0f El sistema de troncos necesita almacenamiento local activo.' }, { quoted: msg });
        return;
    }
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const record = await getModRecord(senderJid);
    const troncos = record?.troncos || 0;
    const mention = `@${getNumberFromJid(senderJid)}`;
    await sock.sendMessage(remoteJid, {
        text: `\ud83e\udeb5 ${mention}, tienes ${troncos} tronco${troncos !== 1 ? 's' : ''} acumulado${troncos !== 1 ? 's' : ''}.`,
        mentions: [senderJid]
    }, { quoted: msg });
}

async function handleDinamicaCommand(sock, msg, remoteJid) {
    const dinamicaText = [
        '🪵 *¿Cómo ganar Troncos?*',
        '',
        'Los troncos son la moneda del estanque. Se ganan cuando tus mensajes reciben reacciones de otros castores.',
        '',
        '🎯 *Reglas:*',
        '',
        '1️⃣ Comparte un aporte de calidad en el grupo.',
        '',
        '2️⃣ Otros miembros deben reaccionar con alguno de estos emojis:',
        '👍 ❤️ 👏 🤯 🔥 💯 🧠 🤖 🦫 💡',
        '',
        '3️⃣ Cuando tu mensaje alcance *5 reacciones* de personas distintas, ganas *1 tronco* 🪵',
        '',
        '4️⃣ Si ese mismo mensaje llega a *10 reacciones*, ganas *1 tronco extra* 🪵🪵',
        '',
        '⚠️ *Importante:*',
        '• Reaccionarte a ti mismo no cuenta.',
        '• Quitar y poner la reacción no suma más.',
        '• Cada persona solo cuenta una vez por mensaje.',
        '',
        '📊 Usa *.top* para ver el ranking y *.troncos* para ver los tuyos.',
        '',
        '¡A construir el dique! 🦫'
    ].join('\n');
    await sock.sendMessage(remoteJid, { text: dinamicaText }, { quoted: msg });
}

const GROQ_API_KEY = process.env.GROQ_API_KEY || '';
const GROQ_MODEL = process.env.GROQ_MODEL || 'llama-3.1-8b-instant';
const GROQ_MODEL_CANDIDATES = (process.env.GROQ_MODEL_CANDIDATES || 'llama-3.1-8b-instant,llama-3.3-70b-versatile,gemma2-9b-it')
    .split(',')
    .map((m) => m.trim())
    .filter(Boolean);

function uniqStrings(values) {
    return [...new Set(values.map((v) => String(v || '').trim()).filter(Boolean))];
}

function normalizeGroqModelName(modelName) {
    return String(modelName || '').trim();
}

function extractGroqResponseText(data) {
    const choice = data?.choices?.[0];
    const message = choice?.message || {};
    const content = message?.content;
    if (typeof content === 'string' && content.trim()) return content.trim();
    if (Array.isArray(content)) {
        const joined = content
            .map((part) => {
                if (typeof part === 'string') return part;
                if (typeof part?.text === 'string') return part.text;
                return '';
            })
            .join(' ')
            .trim();
        if (joined) return joined;
    }
    if (typeof message?.reasoning === 'string' && message.reasoning.trim()) return message.reasoning.trim();
    if (typeof choice?.text === 'string' && choice.text.trim()) return choice.text.trim();
    if (typeof data?.output_text === 'string' && data.output_text.trim()) return data.output_text.trim();
    return '';
}

async function generateAIContent(systemPrompt, userPrompt, maxTokens = 150) {
    if (!GROQ_API_KEY) return null;
    try {
        const models = uniqStrings([GROQ_MODEL, ...GROQ_MODEL_CANDIDATES]).map(normalizeGroqModelName);
        for (const modelName of models) {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 15000);
            const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${GROQ_API_KEY}`
                },
                body: JSON.stringify({
                    model: modelName,
                    messages: [
                        { role: 'system', content: systemPrompt },
                        { role: 'user', content: userPrompt }
                    ],
                    temperature: 0.8,
                    max_tokens: maxTokens
                }),
                signal: controller.signal
            });
            clearTimeout(timeout);

            if (response.ok) {
                const data = await response.json();
                const text = extractGroqResponseText(data);
                if (text) return text;
                const finishReason = data?.choices?.[0]?.finish_reason || 'unknown';
                console.warn(`[PROACTIVO-IA] Respuesta vacía en ${modelName} (groq). finish_reason=${finishReason}. Reintentando con prompt simple.`);

                const retryController = new AbortController();
                const retryTimeout = setTimeout(() => retryController.abort(), 15000);
                const retryResponse = await fetch('https://api.groq.com/openai/v1/chat/completions', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${GROQ_API_KEY}`
                    },
                    body: JSON.stringify({
                        model: modelName,
                        messages: [
                            { role: 'user', content: `${systemPrompt}\n\n${userPrompt}` }
                        ],
                        temperature: 0.4,
                        max_tokens: maxTokens
                    }),
                    signal: retryController.signal
                });
                clearTimeout(retryTimeout);
                if (retryResponse.ok) {
                    const retryData = await retryResponse.json();
                    const retryText = extractGroqResponseText(retryData);
                    if (retryText) return retryText;
                }
                continue;
            }

            const status = response.status;
            const bodyText = await response.text();
            console.error(`[PROACTIVO-IA] Error HTTP ${status} con modelo ${modelName} (groq): ${bodyText.slice(0, 220)}`);

            // Error de modelo: intenta con otro candidato.
            if (status === 400 || status === 404) continue;
            // Error de auth/permisos: no seguir intentando.
            if (status === 401 || status === 403) return null;
        }
        return null;
    } catch (error) {
        console.error('[PROACTIVO-IA] Error generando contenido con Groq:', error?.message || error);
        return null;
    }
}

async function generateDailyChallenge() {
    const categories = [
        'generación de imágenes con IA', 'prompts para texto creativo', 'generación de código',
        'automatización de tareas', 'análisis de datos con IA', 'marketing y redes sociales con IA',
        'escritura y copywriting con IA', 'traducción y localización', 'debugging y revisión de código',
        'educación y aprendizaje con IA', 'productividad personal con IA', 'diseño gráfico con IA',
        'música y audio con IA', 'video y animación con IA', 'chatbots y asistentes',
        'prompt engineering avanzado', 'investigación con IA', 'negocio y emprendimiento con IA',
        'fotografía y edición con IA', 'storytelling y narrativa con IA'
    ];
    const category = categories[Math.floor(Math.random() * categories.length)];
    const systemPrompt = 'Eres un experto en inteligencia artificial y prompt engineering. Tu trabajo es crear retos diarios para un grupo de WhatsApp enfocado en IA y prompts.';
    const userPrompt = `Genera UN reto breve para el "Prompt del Día" de un grupo de IA.

Categoría sugerida: ${category}

Reglas:
- Máximo 2 líneas para el reto
- Debe ser un reto concreto y accionable (NO genérico como "habla de IA")
- Enfocado en crear un prompt específico para una tarea de IA
- Tono motivador y claro
- NO incluyas emojis, títulos, formato, comillas ni texto extra
- Solo devuelve la descripción del reto en una oración

Ejemplo: crear un prompt para que una IA genere un logo minimalista de una startup tech

Genera el reto:`;
    const result = await generateAIContent(systemPrompt, userPrompt, 100);
    if (!result) return null;
    let clean = result.replace(/^["'`\s*•\-]+|["'`\s]+$/g, '').replace(/\n+/g, ' ').trim();
    if (clean.length > 200) clean = clean.slice(0, 197) + '...';
    if (clean.length < 15) return null;
    const emojis = ['🎨', '✍️', '🤖', '💻', '📊', '🧠', '📧', '🌍', '📖', '🐛', '📸', '📚', '🔍', '📱', '🎵', '🏗️', '⚡', '🎯', '🔮', '💡'];
    const emoji = emojis[Math.floor(Math.random() * emojis.length)];
    return { emoji, challenge: clean };
}

async function generateRandomUserTopic() {
    const systemPrompt = 'Eres un community manager de un grupo de WhatsApp sobre inteligencia artificial y prompts.';
    const userPrompt = `Genera UNA pregunta breve para hacerle a un miembro seleccionado al azar del grupo de IA.

Reglas:
- Máximo 1-2 líneas
- Sobre IA, prompts, automatización o herramientas de IA
- Debe invitar a compartir algo útil (experiencia, tip, prompt, herramienta, descubrimiento)
- Tono casual y amigable
- NO incluyas emojis, formato, comillas ni texto extra
- Solo devuelve la pregunta, nada más

Ejemplo: ¿Cuál fue el último prompt que te ahorró tiempo en tu trabajo?

Genera la pregunta:`;
    const result = await generateAIContent(systemPrompt, userPrompt, 80);
    if (!result) return null;
    let clean = result.replace(/^["'`\s*•\-]+|["'`\s]+$/g, '').replace(/\n+/g, ' ').trim();
    if (clean.length > 150) clean = clean.slice(0, 147) + '...';
    if (clean.length < 15) return null;
    return clean;
}

function normalizeShowcaseImageUrl(src, repoDef) {
    const cleanSrc = String(src || '').trim();
    if (!cleanSrc) return '';
    if (cleanSrc.startsWith('http://') || cleanSrc.startsWith('https://')) return cleanSrc;
    if (cleanSrc.startsWith('//')) return `https:${cleanSrc}`;
    const normalizedPath = cleanSrc.replace(/^\.\//, '');
    return `${repoDef.imageBaseUrl}${normalizedPath}`;
}

function extractImagesFromSection(section, repoDef) {
    const htmlImages = [...section.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)].map((m) => m[1]);
    const markdownImages = [...section.matchAll(/!\[[^\]]*\]\(([^)\n]+)\)/g)]
        .map((m) => m[1].replace(/\s+"[^"]*"$/, '').trim());
    const rawImages = [...new Set([...htmlImages, ...markdownImages])];
    return rawImages
        .map((src) => normalizeShowcaseImageUrl(src, repoDef))
        .filter(Boolean);
}

function extractPromptFromSection(section) {
    const promptPatterns = [
        /\*\*(?:Prompt|提示词):?\*\*[\s\S]*?```[^\n]*\n([\s\S]*?)```/i,
        /(?:\*\*(?:Prompt|提示词):?\*\*|(?:^|\n)(?:Prompt|提示词)\s*[:：])[\s\S]*?```[^\n]*\n([\s\S]*?)```/i,
        /\*\*(?:Prompt|提示词):?\*\*\s*\n+([\s\S]*?)(?:\n{2,}---|\n{2,}<a id=|\n{2,}\[⬆️|\n{2,}###|\n{2,}\*\*|$)/i,
        /(?:^|\n)(?:Prompt|提示词)\s*[:：]\s*\n?([\s\S]*?)(?:\n{2,}---|\n{2,}<a id=|\n{2,}\[⬆️|\n{2,}###|\n{2,}\*\*|$)/i
    ];
    for (const pattern of promptPatterns) {
        const match = section.match(pattern);
        if (match?.[1]) {
            const prompt = match[1].trim();
            if (prompt.length >= 10) return prompt;
        }
    }
    return '';
}

function parseCaseStyleShowcases(markdown, repoDef) {
    const showcases = [];
    const sectionRegex = /(?:^|\n)\s*###\s*(?:Example|Case|案例)\s+\d+\s*[:：][\s\S]*?(?=(?:\n\s*###\s*(?:Example|Case|案例)\s+\d+\s*[:：])|$)/gi;
    const sections = markdown.match(sectionRegex) || [];
    let imgMissCount = 0;
    let promptMissCount = 0;

    for (const section of sections) {
        try {
            const titleMatch = section.match(/(?:Example|Case|案例)\s+\d+\s*[:：]\s*(?:\[([^\]]+)\]\([^)]*\)|([^\n(]+?))\s*\(by\s*\[?@?([^\]\)\n]+)/i);
            if (!titleMatch) continue;
            const title = (titleMatch[1] || titleMatch[2] || '').trim();
            const author = (titleMatch[3] || '').replace(/[\])].*$/, '').trim();
            if (!title || !author) continue;

            const imageUrls = extractImagesFromSection(section, repoDef);
            if (imageUrls.length === 0) {
                imgMissCount++;
                continue;
            }

            const prompt = extractPromptFromSection(section);
            if (!prompt) {
                promptMissCount++;
                continue;
            }

            showcases.push({ title, author, imageUrls, prompt });
        } catch (e) {
            continue;
        }
    }

    return { showcases, imgMissCount, promptMissCount };
}

function parseNumberedPromptShowcases(markdown, repoDef) {
    const showcases = [];
    const sectionRegex = /(?:^|\n)\s*###\s+\d+\.\d+\.\s+[^\n]+[\s\S]*?(?=(?:\n\s*###\s+\d+\.\d+\.\s+)|$)/g;
    const sections = markdown.match(sectionRegex) || [];
    let imgMissCount = 0;
    let promptMissCount = 0;

    for (const section of sections) {
        try {
            const titleMatch = section.match(/###\s+\d+\.\d+\.\s+([^\n]+)/);
            if (!titleMatch) continue;
            const title = titleMatch[1].trim();
            if (!title) continue;

            const sourceMatch = section.match(/\*Source:\s*([^\n*]+)\*/i) || section.match(/Source:\s*([^\n]+)/i);
            const authorHandle = sourceMatch?.[1]?.match(/@([a-zA-Z0-9_]+)/)?.[1];
            const author = authorHandle ? `@${authorHandle}` : (sourceMatch?.[1]?.trim() || 'fuente');

            const imageUrls = extractImagesFromSection(section, repoDef);
            if (imageUrls.length === 0) {
                imgMissCount++;
                continue;
            }

            const prompt = extractPromptFromSection(section);
            if (!prompt) {
                promptMissCount++;
                continue;
            }

            showcases.push({ title, author, imageUrls, prompt });
        } catch (e) {
            continue;
        }
    }

    return { showcases, imgMissCount, promptMissCount };
}

function parsePromptShowcases(markdown, repoDef) {
    const finalShowcases = [];
    const seen = new Set();

    const caseStyle = parseCaseStyleShowcases(markdown, repoDef);
    const numbered = parseNumberedPromptShowcases(markdown, repoDef);
    const merged = [...caseStyle.showcases, ...numbered.showcases];

    for (const item of merged) {
        const key = `${item.title.toLowerCase()}|${item.prompt.slice(0, 120).toLowerCase()}`;
        if (seen.has(key)) continue;
        seen.add(key);
        finalShowcases.push(item);
    }

    const imgMissCount = caseStyle.imgMissCount + numbered.imgMissCount;
    const promptMissCount = caseStyle.promptMissCount + numbered.promptMissCount;
    if (imgMissCount > 0 || promptMissCount > 0) {
        console.log(`[SHOWCASE-PARSE] ${repoDef.id}: omitidos sin imagen=${imgMissCount}, sin prompt=${promptMissCount}`);
    }

    return finalShowcases;
}

function detectModelNameFromImageUrl(url) {
    if (!url || typeof url !== 'string') return null;
    const normalized = url.toLowerCase();
    const modelPatterns = [
        { regex: /\bgpt[-_ ]?4\.?1?\b/, label: 'GPT-4.1' },
        { regex: /\bgpt[-_ ]?4o\b/, label: 'GPT-4o' },
        { regex: /\bgpt[-_ ]?4\b/, label: 'GPT-4' },
        { regex: /\bgemini\b/, label: 'Gemini' },
        { regex: /\bclaude\b/, label: 'Claude' },
        { regex: /\bflux\b/, label: 'FLUX' },
        { regex: /\bgrok\b/, label: 'Grok' },
        { regex: /\bmistral\b/, label: 'Mistral' },
        { regex: /\bmidjourney\b|\bmj\b/, label: 'Midjourney' },
        { regex: /\bdall[-_ ]?e\b|\bdalle\b/, label: 'DALL-E' }
    ];

    for (const pattern of modelPatterns) {
        if (pattern.regex.test(normalized)) return pattern.label;
    }
    return null;
}

function normalizeImageLabels(labels, count) {
    if (!Array.isArray(labels) || labels.length !== count) return [];
    return labels.map((label) => {
        const clean = String(label || '').replace(/^["'`]|["'`]$/g, '').trim();
        return clean || '✨ _Resultado_';
    });
}

function buildDefaultImageLabels(imageUrls) {
    const labels = [];
    for (let i = 0; i < imageUrls.length; i++) {
        if (i === 0) labels.push('🖼️ _Imagen de Referencia (Input)_');
        else if (i === imageUrls.length - 1) labels.push('✨ _Imagen Final (Output)_');
        else labels.push(`✨ _Resultado ${i}_`);
    }
    return labels;
}

function buildModelComparisonLabels(imageUrls) {
    if (!Array.isArray(imageUrls) || imageUrls.length < 2) return [];
    const models = imageUrls.map((url) => detectModelNameFromImageUrl(url));
    if (models.some((model) => !model)) return [];
    const distinct = new Set(models);
    if (distinct.size < 2) return [];
    return models.map((model) => `🤖 _${model}_`);
}

function translateTitleFallbackEs(title) {
    const raw = String(title || '').trim();
    if (!raw) return '';
    const phraseRules = [
        { regex: /\bcreate an?\b/gi, replace: 'Crea un' },
        { regex: /\badd\b/gi, replace: 'Agrega' },
        { regex: /\bto\b/gi, replace: 'a' }
    ];
    let normalized = raw;
    for (const rule of phraseRules) {
        normalized = normalized.replace(rule.regex, rule.replace);
    }
    const dictionary = new Map([
        ['add', 'agregar'],
        ['giant', 'gigante'],
        ['creature', 'criatura'],
        ['city', 'ciudad'],
        ['image', 'imagen'],
        ['create', 'crear'],
        ['thoughts', 'pensamientos'],
        ['moving', 'movimiento'],
        ['train', 'tren'],
        ['on', 'sobre'],
        ['an', 'un'],
        ['a', 'un'],
        ['steampunk', 'steampunk'],
        ['tang', 'tang'],
        ['dynasty', 'dinastía'],
        ['song', 'song'],
        ['mechanical', 'mecanico'],
        ['fish', 'pez'],
        ['car', 'coche'],
        ['voxel', 'voxel'],
        ['style', 'estilo'],
        ['icon', 'icono'],
        ['conversion', 'conversion'],
        ['miniature', 'miniatura'],
        ['diorama', 'diorama'],
        ['keyboard', 'teclado'],
        ['keycap', 'tecla'],
        ['esc', 'esc'],
        ['portrait', 'retrato'],
        ['realistic', 'realista'],
        ['cinematic', 'cinematografico'],
        ['photo', 'foto']
    ]);
    const translated = normalized
        .split(/(\s+|[-_/])/)
        .map((chunk) => {
            const lower = chunk.toLowerCase();
            return dictionary.get(lower) || chunk;
        })
        .join('')
        .replace(/\s+/g, ' ')
        .trim();
    return translated.charAt(0).toUpperCase() + translated.slice(1);
}

function looksSpanishText(text) {
    const value = String(text || '').toLowerCase();
    if (!value.trim()) return false;
    if (/[áéíóúñ]/.test(value)) return true;
    const markers = [' de ', ' y ', ' con ', ' para ', ' en ', ' del ', ' la ', ' el '];
    return markers.some((marker) => value.includes(marker));
}

function hasEnglishTitleMarkers(text) {
    const value = String(text || '').toLowerCase();
    const englishMarkers = [
        ' thoughts ', ' moving ', ' train ', ' style ', ' portrait ', ' shot ', ' image ', ' dynasty ',
        ' with ', ' on ', ' by ', ' the ', ' create ', ' add '
    ];
    return englishMarkers.some((marker) => value.includes(marker));
}

function isWeakShowcaseDescription(text) {
    const value = String(text || '').toLowerCase().trim();
    if (!value) return true;
    if (value.length < 28) return true;
    if (value.length > 240) return true;
    const weakPatterns = [
        /este prompt te ayuda/i,
        /con este prompt puedes/i,
        /esta pensado para/i,
        /con buena profundidad y detalles de iluminacion/i
    ];
    return weakPatterns.some((pattern) => pattern.test(value));
}

function cleanModelOutputText(text) {
    return String(text || '')
        .replace(/```json|```/gi, '')
        .replace(/^["'`]|["'`]$/g, '')
        .trim();
}

function tryExtractJsonObject(text) {
    const raw = String(text || '').trim();
    if (!raw) return null;
    const first = raw.indexOf('{');
    const last = raw.lastIndexOf('}');
    if (first === -1 || last === -1 || last <= first) return null;
    const candidate = raw.slice(first, last + 1);
    try {
        return JSON.parse(candidate);
    } catch (_) {
        return null;
    }
}

function extractPromptCues(prompt) {
    const source = String(prompt || '').toLowerCase();
    const cues = [];
    if (source.includes('train')) cues.push('un tren en movimiento');
    if (source.includes('window')) cues.push('ventana lateral');
    if (source.includes('night')) cues.push('escena nocturna');
    if (source.includes('neon')) cues.push('reflejos de neón');
    if (source.includes('bokeh')) cues.push('bokeh colorido');
    if (source.includes('city')) cues.push('luces urbanas');
    if (source.includes('hoodie')) cues.push('persona con sudadera oscura');
    if (source.includes('shallow depth of field')) cues.push('fondo desenfocado');
    if (source.includes('cinematic')) cues.push('estética cinematográfica');
    if (source.includes('tang dynasty')) cues.push('vestimenta de la dinastía Tang');
    if (source.includes('northern song')) cues.push('estilo pictórico de la dinastía Song');
    if (source.includes('oil painting')) cues.push('acabado de pintura al óleo');
    if (source.includes('patrick star')) cues.push('Patrick Star como personaje');
    if (source.includes('hat')) cues.push('sombrero tradicional');
    return [...new Set(cues)].slice(0, 3);
}

function descriptionHasCue(text, cues) {
    const value = String(text || '').toLowerCase();
    if (!Array.isArray(cues) || cues.length < 2) return true;
    return cues.some((cue) => {
        const parts = cue.toLowerCase().split(/\s+/).filter((p) => p.length > 3);
        return parts.some((part) => value.includes(part));
    });
}

async function generateShowcaseDescription(title, prompt) {
    const systemPrompt = "Eres un experto en arte con IA. Describe el resultado del prompt de forma súper concisa, directa y con estilo.";
    const userPrompt = `Redacta una descripción MUY CORTA en español de México (máximo 15 a 20 palabras) sobre la imagen que genera este prompt.
    Solo capta la esencia y el estilo visual, sin enlistar cada detalle técnico.
    
    REGLAS:
    - MÁXIMO 20 palabras. Sé directo, punchy y al grano.
    - Usa español de México fluido y natural.
    - NADA de "Este prompt genera..." o "Una imagen de...". Empieza directo con el concepto (ej. "Selfie fotorrealista de un grupo de amigos con luz natural...").
    
    Título: ${title}
    Prompt: ${prompt}`;

    try {
        const aiResponse = await generateAIContent(systemPrompt, userPrompt, 150);
        const sentence = cleanModelOutputText(aiResponse);

        if (sentence && sentence.length > 10 && sentence.length < 300) {
            return { text: sentence, source: 'ia_corta_mx', reason: 'ok', rawLen: sentence.length };
        }
        return { text: '', source: 'none', reason: 'respuesta_invalida', rawLen: sentence ? sentence.length : 0 };
    } catch (error) {
        return { text: '', source: 'none', reason: 'error_api', rawLen: 0 };
    }
}

function buildShortPromptDescription(title, prompt) {
    const cleanTitle = String(title || '').replace(/[*_`#>\[\]]/g, '').trim();
    const cleanPrompt = String(prompt || '')
        .replace(/[`*_#>]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    const source = `${cleanTitle} ${cleanPrompt}`.toLowerCase();

    let subject = cleanTitle || 'una escena visual';
    const subjectRules = [
        { key: 'patrick star', label: 'Patrick Star con vestimenta histórica' },
        { key: 'emoji', label: 'un emoji' },
        { key: 'rug', label: 'una alfombra artesanal' },
        { key: 'fish', label: 'un pez mecánico' },
        { key: 'car', label: 'un coche personalizado' },
        { key: 'portrait', label: 'un retrato' },
        { key: 'icon', label: 'un ícono' },
        { key: 'diorama', label: 'un diorama miniatura' }
    ];
    for (const rule of subjectRules) {
        if (source.includes(rule.key)) {
            subject = rule.label;
            break;
        }
    }

    const styleParts = [];
    if (source.includes('steampunk')) styleParts.push('con estética steampunk');
    if (source.includes('voxel')) styleParts.push('en estilo voxel 3D');
    if (source.includes('cinematic')) styleParts.push('con look cinematográfico');
    if (source.includes('oil painting')) styleParts.push('en estilo de pintura al óleo');
    if (source.includes('tang dynasty') || source.includes('northern song')) styleParts.push('con inspiración histórica de dinastías chinas');
    if (source.includes('realistic') || source.includes('realista')) styleParts.push('con acabado realista');
    if (source.includes('tuft') || source.includes('fluffy') || source.includes('rug')) styleParts.push('con textura esponjosa y artesanal');
    if (styleParts.length === 0) styleParts.push('con estilo visual detallado');

    let framing = 'con encuadre visual cuidado';
    if (source.includes('top view') || source.includes('from above') || source.includes('vista desde arriba')) {
        framing = 'en vista desde arriba';
    } else if (source.includes('close-up') || source.includes('macro')) {
        framing = 'en primer plano';
    } else if (source.includes('isometric')) {
        framing = 'con encuadre isométrico';
    }

    const cues = extractPromptCues(cleanPrompt);
    const cueText = cues.length > 0 ? `, destacando ${cues.join(', ')}` : '';
    return `Un prompt para generar ${subject}, ${styleParts.join(', ')} y ${framing}${cueText}.`;
}

async function fetchShowcaseData(repoDef) {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 20000);
        const response = await fetch(repoDef.url, { signal: controller.signal });
        clearTimeout(timeout);
        if (!response.ok) return [];
        const markdown = await response.text();
        const parsed = parsePromptShowcases(markdown, repoDef);
        console.log(`[SHOWCASE] Parseados ${parsed.length} ejemplos del repo ${repoDef.id}`);
        return parsed;
    } catch (error) {
        console.error(`[SHOWCASE] Error descargando repo ${repoDef.id}:`, error?.message || error);
        return [];
    }
}

async function sendPromptShowcase(sock) {
    if (PROACTIVE_GROUP_JIDS.length === 0) return;
    try {
        const state = getProactiveState();
        let tracking = state.showcaseTracking || { picotrex: [], jimmylv: [] };
        
        // Alternar repositorios
        const totalSent = (tracking.picotrex?.length || 0) + (tracking.jimmylv?.length || 0);
        const repoIndex = totalSent % SHOWCASE_REPOS.length;
        const repoDef = SHOWCASE_REPOS[repoIndex];
        
        if (!tracking[repoDef.id]) tracking[repoDef.id] = [];
        const sentIndices = tracking[repoDef.id];
        
        const showcases = await fetchShowcaseData(repoDef);
        if (!showcases || showcases.length === 0) {
            console.log(`[SHOWCASE] No hay showcases disponibles en ${repoDef.id}`);
            return;
        }
        
        let available = showcases.map((s, i) => i).filter((i) => !sentIndices.includes(i));
        if (available.length === 0) {
            available = showcases.map((s, i) => i);
            tracking[repoDef.id] = [];
            console.log(`[SHOWCASE] Todos los showcases enviados para ${repoDef.id}, reiniciando rotación`);
        }
        
        // Si un caso no consigue descripción IA, intenta otros casos del mismo repo.
        const candidateIndices = [...available].sort(() => Math.random() - 0.5);
        const maxAttempts = Math.min(candidateIndices.length, 6);
        let selectedIndex = -1;
        let showcase = null;
        let finalTitle = '';
        let finalPrompt = '';
        let finalDescription = '';
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            selectedIndex = candidateIndices[attempt];
            showcase = showcases[selectedIndex];
            finalTitle = showcase.title;
            finalPrompt = showcase.prompt;

            const translatedTitle = await generateAIContent(
                "Como traductor, traduce el siguiente título a español. Importante: Devuelve SOLO el título traducido, sin comillas, ni asteriscos ni texto adicional.",
                showcase.title,
                100
            );
            if (translatedTitle) finalTitle = translatedTitle.replace(/^["'`]|["'`]$/g, '').trim();
            if (!looksSpanishText(finalTitle) || hasEnglishTitleMarkers(` ${finalTitle} `)) {
                const fallbackTitle = translateTitleFallbackEs(showcase.title);
                if (fallbackTitle) finalTitle = fallbackTitle;
            }

            const engPrompt = await generateAIContent(
                "Translate this prompt accurately to English. If it is already in English, output it exactly as is. Don't add quotes or any markdown. Output ONLY the prompt.",
                showcase.prompt,
                1500
            );
            if (engPrompt) finalPrompt = engPrompt;

            const descriptionResult = await generateShowcaseDescription(finalTitle, finalPrompt);
            const descriptionAI = descriptionResult?.text || '';
            finalDescription = descriptionAI;
            const debugCues = extractPromptCues(finalPrompt);
            console.log(`[SHOWCASE-DESC] ${repoDef.id}: fuente=${descriptionAI ? 'ia' : 'sin_descripcion'} origen=${descriptionResult?.source || 'none'} motivo=${descriptionResult?.reason || 'unknown'} cues=${debugCues.length || 0} prompt_chars=${finalPrompt.length} desc_chars=${finalDescription.length} intento=${attempt + 1}/${maxAttempts}`);
            if (finalDescription) {
                break;
            }
            console.log(`[SHOWCASE] Omitido: sin descripcion IA valida para "${showcase.title}" (repo: ${repoDef.id})`);
        }
        if (!finalDescription || !showcase || selectedIndex < 0) {
            console.log(`[SHOWCASE] Usando fallback manual para descripción tras ${maxAttempts} intentos fallidos con la IA.`);
            finalDescription = buildShortPromptDescription(finalTitle, finalPrompt);
        }

        const isLongPrompt = finalPrompt.length > SHOWCASE_PROMPT_INLINE_MAX_LENGTH;
        const captionLines = [
            `${CASTOR_EMOJI} *✨ Showcase del Día*`,
            '',
            `📌 *${finalTitle}*`,
            `👤 por ${showcase.author}`,
        ];

        captionLines.push(`ℹ️ ${finalDescription}`);

        if (!isLongPrompt) {
            captionLines.push('', '📝 *Prompt:*', finalPrompt);
        } else {
            captionLines.push('', '📝 El prompt completo se envía como archivo adjunto ⬇️');
        }
        captionLines.push('', '💡 ¡Prueba este prompt en tu IA favorita y comparte el resultado! 👇');
        const captionText = captionLines.join('\n');
        
        let imageLabels = [];
        if (showcase.imageUrls.length > 1) {
            try {
                const fileNames = showcase.imageUrls.map(u => u.substring(u.lastIndexOf('/') + 1));
                const labelingPrompt = `Analiza el caso y genera etiquetas para sus imágenes.
Título: ${finalTitle}
Prompt: ${finalPrompt}
Nombres de archivo: ${fileNames.join(', ')}

Si es una transformación de una imagen a otra, responde con cosas parecidas a "🖼️ Imagen de Referencia (Input)" y "✨ Imagen Final (Output)".
Si es una comparación entre modelos de IA (ej. Gemini vs GPT-4o, donde cada imagen es de un modelo distinto), usa el nombre de los modelos (ej. "🤖 Gemini" y "🤖 GPT-4o") inferidos de los nombres de archivo.

REGLA ESTRICTA: Devuelve ÚNICAMENTE un arreglo en formato JSON válido con exactamente ${showcase.imageUrls.length} strings. Ejemplo de salida: ["etiqueta 1", "etiqueta 2"]`;
                
                const labelsResult = await generateAIContent("Eres un experto clasificador de datos que devuelve puramente JSON.", labelingPrompt, 200);
                if (labelsResult) {
                    const parsed = JSON.parse(labelsResult.replace(/```json|```/g, '').trim());
                    imageLabels = normalizeImageLabels(parsed, showcase.imageUrls.length);
                }
            } catch (e) {
                console.error('[SHOWCASE-LABELS] Parsing falló:', e?.message);
            }
        }

        // Prioriza detección determinística para comparaciones entre IAs.
        if (showcase.imageUrls.length > 1) {
            const modelComparisonLabels = buildModelComparisonLabels(showcase.imageUrls);
            if (modelComparisonLabels.length === showcase.imageUrls.length) {
                imageLabels = modelComparisonLabels;
            }
        }

        if (showcase.imageUrls.length > 1 && imageLabels.length === 0) {
            imageLabels = buildDefaultImageLabels(showcase.imageUrls);
        }
        
        for (const groupJid of PROACTIVE_GROUP_JIDS) {
            try {
                if (showcase.imageUrls.length > 1) {
                    // Enviar texto principal separado si hay múltiples imágenes
                    await sock.sendMessage(groupJid, { text: captionText });
                    
                    // Enviar imágenes en orden
                    for (let i = 0; i < showcase.imageUrls.length; i++) {
                        let imgCaption = imageLabels[i];
                        
                        try {
                            await sock.sendMessage(groupJid, {
                                image: { url: showcase.imageUrls[i] },
                                caption: imgCaption
                            });
                        } catch (imgError) {
                            console.error(`[SHOWCASE] Error enviando múltiple imagen ${i} a ${groupJid}:`, imgError?.message);
                        }
                    }
                } else {
                    // Si hay solo una imagen, enviamos todo junto como caption
                    try {
                        await sock.sendMessage(groupJid, {
                            image: { url: showcase.imageUrls[0] },
                            caption: captionText
                        });
                    } catch (imgError) {
                        console.error(`[SHOWCASE] Error enviando imagen única a ${groupJid}:`, imgError?.message);
                        await sock.sendMessage(groupJid, { text: captionText });
                    }
                }
                
                if (isLongPrompt) {
                    const fileName = `prompt_${finalTitle.replace(/[^a-zA-Z0-9]+/g, '_').slice(0, 40)}.txt`;
                    const fileContent = [
                        `=== PROMPT SHOWCASE: ${finalTitle} ===`,
                        `Autor: ${showcase.author}`,
                        `Repo: ${repoDef.id}`,
                        '',
                        '=== PROMPT ===',
                        '',
                        finalPrompt
                    ].join('\n');
                    await sock.sendMessage(groupJid, {
                        document: Buffer.from(fileContent, 'utf-8'),
                        mimetype: 'text/plain',
                        fileName: fileName
                    });
                }
            } catch (groupError) {
                console.error(`[SHOWCASE] Error enviando a ${groupJid}:`, groupError?.message);
            }
            if (PROACTIVE_GROUP_JIDS.length > 1) await new Promise((r) => setTimeout(r, 3000));
        }
        
        tracking[repoDef.id].push(selectedIndex);
        updateProactiveState({
            lastShowcaseSentAt: new Date().toISOString(),
            showcaseTracking: tracking
        });
        console.log(`[SHOWCASE] Enviado: "${finalTitle}" (repo: ${repoDef.id}, quedan ${available.length - 1} sin enviar)`);
    } catch (error) {
        console.error('[SHOWCASE] Error enviando showcase:', error?.message || error);
    }
}

async function sendPromptOfTheDay(sock) {
    if (PROACTIVE_GROUP_JIDS.length === 0) return;
    try {
        let emoji = '🎯';
        let challenge = '';
        const aiChallenge = await generateDailyChallenge();
        if (aiChallenge) {
            emoji = aiChallenge.emoji;
            challenge = aiChallenge.challenge;
            console.log('[PROACTIVO] Reto generado por IA');
        } else {
            const state = getProactiveState();
            let index = Math.floor(Math.random() * PROACTIVE_PROMPT_CHALLENGES.length);
            if (state.lastPromptIndex === index && PROACTIVE_PROMPT_CHALLENGES.length > 1) {
                index = (index + 1) % PROACTIVE_PROMPT_CHALLENGES.length;
            }
            const fallback = PROACTIVE_PROMPT_CHALLENGES[index];
            emoji = fallback.emoji;
            challenge = fallback.challenge;
            updateProactiveState({ lastPromptIndex: index });
            console.log(`[PROACTIVO] Usando reto local (fallback index: ${index})`);
        }
        const text = [
            `${CASTOR_EMOJI} *Reto del Día* ${emoji}`,
            '',
            'Crea un prompt para:',
            `👉 ${challenge}`,
            '',
            '💡 El mejor prompt gana 🪵 +10',
            '',
            '¡Compartan sus propuestas! 👇'
        ].join('\n');
        for (const groupJid of PROACTIVE_GROUP_JIDS) {
            try {
                await sock.sendMessage(groupJid, { text });
            } catch (groupError) {
                console.error(`[PROACTIVO] Error enviando prompt a ${groupJid}:`, groupError?.message);
            }
            if (PROACTIVE_GROUP_JIDS.length > 1) await new Promise((r) => setTimeout(r, 3000));
        }
        updateProactiveState({ lastPromptSentAt: new Date().toISOString() });
        console.log('[PROACTIVO] Prompt del día enviado');
    } catch (error) {
        console.error('[PROACTIVO] Error enviando prompt del día:', error?.message || error);
    }
}

async function sendRandomUserSelection(sock) {
    if (PROACTIVE_GROUP_JIDS.length === 0) return;
    for (const groupJid of PROACTIVE_GROUP_JIDS) {
        try {
            const metadata = await sock.groupMetadata(groupJid);
            const candidates = (metadata.participants || []).filter((p) => {
                if (p.id === sock.user?.id) return false;
                if (p.admin === 'admin' || p.admin === 'superadmin') return false;
                return true;
            });
            if (candidates.length === 0) {
                console.log(`[PROACTIVO] No hay candidatos en ${groupJid}.`);
                continue;
            }
            const selected = candidates[Math.floor(Math.random() * candidates.length)];
            const displayName = getParticipantDisplayName(selected, selected.id);
            const mentionLabel = getParticipantMentionLabel(selected, selected.id);
            let topic = '';
            const aiTopic = await generateRandomUserTopic();
            if (aiTopic) {
                topic = aiTopic;
                console.log('[PROACTIVO] Tema random generado por IA');
            } else {
                topic = PROACTIVE_USER_TOPICS[Math.floor(Math.random() * PROACTIVE_USER_TOPICS.length)];
                console.log('[PROACTIVO] Usando tema random local (fallback)');
            }
            const text = [
                `${CASTOR_EMOJI} *Castor seleccionó a alguien...*`,
                '',
                `@${mentionLabel} cuéntanos algo:`,
                `👉 ${topic}`,
                '',
                '¡Tu aporte vale 🪵 troncos! Reaccionen a su mensaje 👆'
            ].join('\n');
            await sock.sendMessage(groupJid, { text, mentions: [selected.id] });
            console.log(`[PROACTIVO] Selección aleatoria enviada en ${groupJid}: ${mentionLabel}`);
        } catch (groupError) {
            console.error(`[PROACTIVO] Error en selección aleatoria para ${groupJid}:`, groupError?.message);
        }
        if (PROACTIVE_GROUP_JIDS.length > 1) await new Promise((r) => setTimeout(r, 3000));
    }
    updateProactiveState({ lastRandomUserAt: new Date().toISOString() });
}

async function sendInactivityReactivation(sock) {
    if (PROACTIVE_GROUP_JIDS.length === 0) return;
    try {
        const message = PROACTIVE_REACTIVATION_MESSAGES[Math.floor(Math.random() * PROACTIVE_REACTIVATION_MESSAGES.length)];
        const text = `${CASTOR_EMOJI} ${message}`;
        for (const groupJid of PROACTIVE_GROUP_JIDS) {
            try {
                await sock.sendMessage(groupJid, { text });
            } catch (groupError) {
                console.error(`[PROACTIVO] Error enviando reactivación a ${groupJid}:`, groupError?.message);
            }
            if (PROACTIVE_GROUP_JIDS.length > 1) await new Promise((r) => setTimeout(r, 3000));
        }
        updateProactiveState({ lastReactivationAt: new Date().toISOString() });
        lastGroupActivityAt = Date.now();
        console.log('[PROACTIVO] Mensaje de reactivación enviado');
    } catch (error) {
        console.error('[PROACTIVO] Error en mensaje de reactivación:', error?.message || error);
    }
}

function startProactiveScheduler(sock) {
    if (!PROACTIVE_ENABLED || PROACTIVE_GROUP_JIDS.length === 0) {
        console.log('[PROACTIVO] Sistema deshabilitado o sin grupo configurado.');
        return;
    }
    stopProactiveScheduler();
    const state = getProactiveState();
    if (state.lastGroupActivityAt) {
        lastGroupActivityAt = Math.max(lastGroupActivityAt, new Date(state.lastGroupActivityAt).getTime());
    }
    if (!lastGroupActivityAt) {
        lastGroupActivityAt = Date.now();
    }
    console.log(`[PROACTIVO] Scheduler iniciado. Grupos: ${PROACTIVE_GROUP_JIDS.join(', ')}`);
    console.log(`[PROACTIVO] Prompt: cada ${PROACTIVE_PROMPT_INTERVAL_MS / 3600000}h | Random: cada ${PROACTIVE_RANDOM_USER_INTERVAL_MS / 3600000}h | Inactividad: ${PROACTIVE_INACTIVITY_THRESHOLD_MS / 3600000}h`);
    console.log(`[PROACTIVO] Ventana nocturna: ${PROACTIVE_NIGHT_START_HOUR}:00 - ${PROACTIVE_NIGHT_END_HOUR}:00 (CDMX)`);
    proactiveCheckInterval = setInterval(async () => {
        if (activeSock !== sock) return;
        if (isNightTime()) return;
        if (proactiveCheckRunning) return;
        proactiveCheckRunning = true;
        try {
            const now = Date.now();
            const currentState = getProactiveState();
            if (lastGroupActivityAt > 0) {
                const savedTs = currentState.lastGroupActivityAt ? new Date(currentState.lastGroupActivityAt).getTime() : 0;
                if (lastGroupActivityAt > savedTs) {
                    updateProactiveState({ lastGroupActivityAt: new Date(lastGroupActivityAt).toISOString() });
                }
            }
            const lastShowcase = currentState.lastShowcaseSentAt ? new Date(currentState.lastShowcaseSentAt).getTime() : 0;
            if (now - lastShowcase >= PROACTIVE_SHOWCASE_INTERVAL_MS) {
                const jitter = getRandomDelay(0, PROACTIVE_JITTER_MS);
                await new Promise((resolve) => setTimeout(resolve, jitter));
                if (activeSock === sock && !isNightTime()) {
                    await sendPromptShowcase(sock);
                }
                return;
            }
            const lastPrompt = currentState.lastPromptSentAt ? new Date(currentState.lastPromptSentAt).getTime() : 0;
            const timeSinceShowcase = now - lastShowcase;
            if (now - lastPrompt >= PROACTIVE_PROMPT_INTERVAL_MS && timeSinceShowcase >= PROACTIVE_SHOWCASE_PROMPT_GAP_MS) {
                const jitter = getRandomDelay(0, PROACTIVE_JITTER_MS);
                await new Promise((resolve) => setTimeout(resolve, jitter));
                if (activeSock === sock && !isNightTime()) {
                    await sendPromptOfTheDay(sock);
                }
                return;
            }
            const lastRandom = currentState.lastRandomUserAt ? new Date(currentState.lastRandomUserAt).getTime() : 0;
            if (now - lastRandom >= PROACTIVE_RANDOM_USER_INTERVAL_MS) {
                const jitter = getRandomDelay(0, PROACTIVE_JITTER_MS);
                await new Promise((resolve) => setTimeout(resolve, jitter));
                if (activeSock === sock && !isNightTime()) {
                    await sendRandomUserSelection(sock);
                }
                return;
            }
            const lastReactivation = currentState.lastReactivationAt ? new Date(currentState.lastReactivationAt).getTime() : 0;
            const timeSinceActivity = now - lastGroupActivityAt;
            const timeSinceReactivation = now - lastReactivation;
            if (timeSinceActivity >= PROACTIVE_INACTIVITY_THRESHOLD_MS && timeSinceReactivation >= PROACTIVE_INACTIVITY_THRESHOLD_MS) {
                await sendInactivityReactivation(sock);
            }
        } catch (error) {
            console.error('[PROACTIVO] Error en ciclo de verificación:', error?.message || error);
        } finally {
            proactiveCheckRunning = false;
        }
    }, 5000);
}

function stopProactiveScheduler() {
    if (proactiveCheckInterval) {
        clearInterval(proactiveCheckInterval);
        proactiveCheckInterval = null;
        proactiveCheckRunning = false;
        console.log('[PROACTIVO] Scheduler detenido.');
    }
}

app.get('/', (req, res) => {
    if (qrCodeData) {
        res.send(`
            <html>
                <head>
                    <title>Bot WhatsApp - QR</title>
                    <meta http-equiv="refresh" content="5">
                    <style>body{font-family:sans-serif;text-align:center;padding:50px;}</style>
                </head>
                <body>
                    <h1>🤖 Escanea el QR</h1>
                    <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(qrCodeData)}" />
                </body>
            </html>
        `);
        return;
    }

    res.send(`
        <html>
            <head><title>Bot Activo</title></head>
            <body style="font-family:sans-serif;text-align:center;padding:50px;">
                <h1>✅ Bot conectado</h1>
            </body>
        </html>
    `);
});

app.listen(PORT, () => console.log(`Servidor iniciado en puerto ${PORT}`));

function startKeepAlive() {
    if (keepAliveInterval) {
        return;
    }
    keepAliveInterval = setInterval(async () => {
        const renderUrl = process.env.RENDER_EXTERNAL_URL;
        if (!renderUrl) {
            return;
        }
        try {
            await fetch(renderUrl);
        } catch (error) {
        }
    }, KEEP_ALIVE_INTERVAL_MS);
}

function touchSocketActivity() {
    lastSocketActivityAt = Date.now();
}

function startHealthWatchdog() {
    if (healthWatchInterval) {
        return;
    }
    healthWatchInterval = setInterval(() => {
        if (!activeSock || isStartingBot) {
            return;
        }
        const idleMs = Date.now() - lastSocketActivityAt;
        if (idleMs >= BOT_STALE_SOCKET_MS) {
            const lastCommandAgo = lastCommandHandledAt ? `${Date.now() - lastCommandHandledAt}ms` : 'sin comandos previos';
            console.log(`Watchdog detectó socket inactivo por ${idleMs}ms. Último comando: ${lastCommandAgo}. Reiniciando conexión.`);
            scheduleReconnect('watchdog_stale_socket');
        }
    }, BOT_HEALTHCHECK_INTERVAL_MS);
}

function stopConnectionIntervals() {
    if (antiSleepInterval) {
        clearInterval(antiSleepInterval);
        antiSleepInterval = null;
    }
    if (presenceKeepAliveInterval) {
        clearInterval(presenceKeepAliveInterval);
        presenceKeepAliveInterval = null;
    }
    stopProactiveScheduler();
}

function startConnectionIntervals(sock) {
    stopConnectionIntervals();
    let lastTickAt = Date.now();

    antiSleepInterval = setInterval(() => {
        const now = Date.now();
        const diff = now - lastTickAt;
        lastTickAt = now;
        if (diff > BOT_SUSPEND_DETECTION_MS) {
            console.log(`Detección de suspensión/lag por ${diff}ms. Forzando reconexión preventiva.`);
            scheduleReconnect('suspend_detected');
        }
    }, BOT_SUSPEND_CHECK_INTERVAL_MS);

    presenceKeepAliveInterval = setInterval(async () => {
        if (activeSock !== sock) {
            return;
        }
        try {
            await sock.sendPresenceUpdate('available');
            touchSocketActivity();
        } catch (error) {
            scheduleReconnect('presence_keepalive_failed');
        }
    }, BOT_PRESENCE_KEEPALIVE_MS);
}

function getErrorMessage(error) {
    return String(error?.message || error || '').toLowerCase();
}

function isTimeoutLikeError(error) {
    const statusCode = error?.output?.statusCode || error?.data?.statusCode || error?.statusCode;
    if (statusCode === 408) {
        return true;
    }
    const msg = getErrorMessage(error);
    return msg.includes('timed out') || msg.includes('request time-out') || msg.includes('timeout');
}

function isAuthCryptoError(error) {
    const msg = getErrorMessage(error);
    return msg.includes('unsupported state or unable to authenticate data')
        || msg.includes('bad decrypt')
        || msg.includes('invalid mac');
}

function isSocketOpen(sock) {
    return !!sock?.ws && sock.ws.readyState === 1;
}

async function withTimeout(promise, ms, label) {
    let timer = null;
    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(`${label} timeout`)), ms);
            })
        ]);
    } finally {
        if (timer) {
            clearTimeout(timer);
        }
    }
}

function getMessageTimestampMs(msg) {
    const rawTimestamp = msg?.messageTimestamp;
    if (!rawTimestamp) {
        return 0;
    }
    const timestamp = typeof rawTimestamp === 'number'
        ? rawTimestamp
        : typeof rawTimestamp?.low === 'number'
            ? rawTimestamp.low
            : Number(rawTimestamp) || 0;
    return timestamp > 0 ? timestamp * 1000 : 0;
}

async function processIncomingMessage(sock, msg, runId) {
    if (runId !== botRunId || !msg?.message) {
        return;
    }
    if (msg.key.fromMe && !ALLOW_SELF_COMMANDS) {
        return;
    }
    if (msg.message?.reactionMessage) {
        await handleReactionForTroncos(sock, msg);
        return;
    }
    if (msg.message.protocolMessage) {
        return;
    }

    const messageTime = getMessageTimestampMs(msg);
    if (messageTime) {
        const ageMs = Date.now() - messageTime;
        if (ageMs > 2 * 60 * 1000) {
            return;
        }
    }

    const remoteJid = msg.key.remoteJid;
    if (!remoteJid) {
        return;
    }
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const text = extractTextFromMessage(msg.message);
    let senderIsAdmin = false;
    if (remoteJid.endsWith('@g.us')) {
        try {
            senderIsAdmin = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
        } catch (error) {
            senderIsAdmin = false;
        }
    }

    touchLastActivityAsync(senderJid);
    touchGroupActivity(remoteJid);
    try {
        await sock.readMessages([msg.key]);
    } catch (error) {
    }

    if (remoteJid.endsWith('@g.us') && text && hasGroupInviteLink(text) && !senderIsAdmin) {
        try {
            await sock.sendMessage(remoteJid, { delete: msg.key });
        } catch (error) {
        }
        if (isStorageReady) {
            const autoWarnResult = await applyWarning(sock, senderJid, remoteJid, 'Invitación de grupo detectada automáticamente');
            await sendCastorSealSticker(sock, remoteJid, msg);
            const mention = `@${getNumberFromJid(senderJid)}`;
            const warningText = autoWarnResult.warningCount < 3
                ? `🚫 ${mention}, las invitaciones a otros grupos están prohibidas. Has sido advertido automáticamente.`
                : `🚫 ${mention}, las invitaciones a otros grupos están prohibidas. Alcanzaste 3/3 advertencias.`;
            await sock.sendMessage(remoteJid, { text: warningText, mentions: [senderJid] });
        } else {
            await sock.sendMessage(remoteJid, { text: '🚫 Las invitaciones a otros grupos están prohibidas.' });
        }
        return;
    }

    if (!text || !text.trim().startsWith('.')) {
        return;
    }

    const command = text.trim().split(/\s+/)[0].toLowerCase();
    if (CASTOR_VALID_COMMANDS.has(command) && !SAFE_DISABLE_COMMAND_REACT) {
        setTimeout(() => {
            sock.sendMessage(remoteJid, { react: { text: CASTOR_EMOJI, key: msg.key } }).catch(() => {});
        }, getRandomDelay(500, 1500));
    }
    lastCommandHandledAt = Date.now();
    if (command === '.reportar') {
        await handleReportCommand(sock, msg, text, remoteJid);
    } else if (command === '.advertir') {
        await handleWarnCommand(sock, msg, text, remoteJid);
    } else if (command === '.ban') {
        await handleBanCommand(sock, msg, text, remoteJid);
    } else if (command === '.unban') {
        await handleUnbanCommand(sock, msg, text, remoteJid);
    } else if (command === '.sticker') {
        await handleStickerCommand(sock, msg, remoteJid);
    } else if (command === '.top') {
        await handleTopCommand(sock, msg, remoteJid);
    } else if (command === '.random') {
        await handleRandomCommand(sock, msg, remoteJid);
    } else if (command === '.fantasmas') {
        await handleGhostsCommand(sock, msg, text, remoteJid);
    } else if (command === '.cerrar') {
        await handleCloseGroupCommand(sock, msg, text, remoteJid);
    } else if (command === '.abrir') {
        await handleOpenGroupCommand(sock, msg, remoteJid);
    } else if (command === '.ping') {
        await handlePingCommand(sock, msg, remoteJid);
    } else if (command === '.comandos') {
        await handleCommandsListCommand(sock, msg, remoteJid);
    } else if (command === '.reglas') {
        await handleRulesCommand(sock, msg, remoteJid);
    } else if (command === '.miid') {
        await handleMyIdCommand(sock, msg, remoteJid);
    } else if (command === '.setadmin') {
        await handleSetAdminCommand(sock, msg, remoteJid);
    } else if (command === '.troncos') {
        await handleTroncosCommand(sock, msg, remoteJid);
    } else if (command === '.dinamica') {
        await handleDinamicaCommand(sock, msg, remoteJid);
    } else if (command === '.grupoid') {
        const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
        if (!isAuthorized) {
            await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        } else if (!remoteJid.endsWith('@g.us')) {
            await sock.sendMessage(remoteJid, { text: 'Este comando solo funciona en grupos.' }, { quoted: msg });
        } else {
            const senderPrivateJid = msg.key.participant || msg.key.remoteJid;
            await sock.sendMessage(senderPrivateJid, { text: `📋 JID del grupo:\n\n${remoteJid}\n\nCopia este valor en PROACTIVE_GROUP_JID de tu configuración.` });
            await sock.sendMessage(remoteJid, { text: '📋 Información enviada por privado.' }, { quoted: msg });
        }
    }
}

async function processIncomingQueue(sock, runId) {
    if (isProcessingIncoming || incomingQueue.length === 0) {
        return;
    }
    isProcessingIncoming = true;
    try {
        while (incomingQueue.length > 0) {
            const msg = incomingQueue.shift();
            try {
                await processIncomingMessage(sock, msg, runId);
            } catch (error) {
                const errorMessage = error?.message || String(error || '');
                if (!/connection closed|socket no disponible/i.test(errorMessage)) {
                    console.error('Error en procesamiento de comando:', errorMessage);
                }
            }
        }
    } finally {
        isProcessingIncoming = false;
        if (incomingQueue.length > 0) {
            processIncomingQueue(sock, runId).catch(() => {});
        }
    }
}

function scheduleReconnect(reason) {
    if (reconnectInProgress || reconnectTimer) {
        return;
    }
    reconnectInProgress = true;
    stopConnectionIntervals();
    if (activeSock) {
        try {
            activeSock.end(new Error('reconnect_requested'));
        } catch (error) {
        }
        activeSock = null;
    }
    const wait = reconnectDelayMs;
    reconnectDelayMs = Math.min(BOT_RECONNECT_MAX_MS, reconnectDelayMs * 2);
    console.log(`Reintentando conexión en ${wait}ms. Motivo: ${reason}`);
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        if (activeSock && isSocketOpen(activeSock)) {
            reconnectInProgress = false;
            return;
        }
        startBot().catch((error) => {
            console.error('Error al reintentar inicio del bot:', error?.message || error);
            reconnectInProgress = false;
            scheduleReconnect('fallo_reintento');
        });
    }, wait);
}

function setupProcessErrorGuard() {
    if (processErrorGuardReady) {
        return;
    }
    processErrorGuardReady = true;
    process.on('unhandledRejection', (reason) => {
        console.error('UnhandledRejection detectado:', reason?.message || reason);
        if (isTimeoutLikeError(reason) || isAuthCryptoError(reason)) {
            scheduleReconnect('unhandled_connection_issue');
        }
    });
    process.on('uncaughtException', (error) => {
        console.error('UncaughtException detectado:', error?.message || error);
        if (isTimeoutLikeError(error) || isAuthCryptoError(error)) {
            scheduleReconnect('uncaught_connection_issue');
            return;
        }
        scheduleReconnect('uncaught_error');
    });
    process.on('SIGTERM', () => {
        console.error('SIGTERM detectado: Render o el contenedor terminó el proceso.');
        stopConnectionIntervals();
    });
    process.on('SIGINT', () => {
        console.error('SIGINT detectado: detención manual del proceso.');
        stopConnectionIntervals();
    });
}

async function startBot() {
    if (isStartingBot) {
        return;
    }
    isStartingBot = true;
    const runId = ++botRunId;
    setupProcessErrorGuard();
    await ensureLocalStorage();
    try {
        if (activeSock) {
            try {
                activeSock.end(new Error('new_run_started'));
            } catch (error) {
            }
        }

        if (RESET_WA_SESSION_ON_BOOT && !sessionResetDoneThisBoot) {
            try {
                fs.rmSync('auth_info_baileys', { recursive: true, force: true });
                console.log('RESET_WA_SESSION_ON_BOOT activo: auth_info_baileys eliminado.');
            } catch (error) {
            }
            sessionResetDoneThisBoot = true;
        }

        let state;
        let saveCreds;
        const auth = await useMultiFileAuthState('auth_info_baileys');
        state = auth.state;
        saveCreds = auth.saveCreds;
        console.log('Sesión de WhatsApp usando archivos locales');

        const { version } = await fetchLatestBaileysVersion();

        const sock = makeWASocket({
            version,
            logger: pino({ level: 'info' }),
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
            },
            browser: Browsers.ubuntu('Chrome'),
            connectTimeoutMs: BAILEYS_CONNECT_TIMEOUT_MS,
            defaultQueryTimeoutMs: BAILEYS_QUERY_TIMEOUT_MS,
            keepAliveIntervalMs: BAILEYS_KEEPALIVE_MS,
            syncFullHistory: false,
            markOnlineOnConnect: true,
            retryRequestDelayMs: BAILEYS_RETRY_REQUEST_DELAY_MS
        });
        activeSock = sock;

        const originalSendMessage = sock.sendMessage.bind(sock);
        sock.sendMessage = (jid, content, options) => {
            if (content?.react || content?.delete) {
                return withTimeout(originalSendMessage(jid, content, options), SEND_ACTION_TIMEOUT_MS, 'send_action')
                    .then((result) => {
                        touchSocketActivity();
                        return result;
                    });
            }
            const normalizedContent = { ...(content || {}) };
            if (typeof normalizedContent.text === 'string') {
                normalizedContent.text = brandCastorText(normalizedContent.text);
            }
            if (typeof normalizedContent.caption === 'string') {
                normalizedContent.caption = brandCastorText(normalizedContent.caption);
            }
            const task = async () => {
                const now = Date.now();
                const lastAt = lastSentAtByJid.get(jid) || 0;
                const missingGap = Math.max(0, PER_CHAT_MIN_GAP_MS - (now - lastAt));
                const randomDelay = getRandomDelay(SEND_MIN_DELAY_MS, SEND_MAX_DELAY_MS);
                const totalDelay = missingGap + randomDelay;
                if (totalDelay > 0) {
                    try {
                        await sock.sendPresenceUpdate('composing', jid);
                    } catch (error) {
                    }
                    await new Promise((resolve) => setTimeout(resolve, totalDelay));
                    try {
                        await sock.sendPresenceUpdate('paused', jid);
                    } catch (error) {
                    }
                }
                const result = await withTimeout(originalSendMessage(jid, normalizedContent, options), SEND_ACTION_TIMEOUT_MS, 'send_message');
                touchSocketActivity();
                lastSentAtByJid.set(jid, Date.now());
                return result;
            };
            const queuedResult = globalSendQueue.catch(() => null).then(task);
            globalSendQueue = queuedResult.catch((error) => {
                if (/timeout|connection closed|socket no disponible/i.test(error?.message || '')) {
                    scheduleReconnect(error?.message || 'send_failure');
                }
                return null;
            });
            return queuedResult;
        };

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', (update) => {
            if (runId !== botRunId) {
                return;
            }
            touchSocketActivity();
            const { connection, lastDisconnect, qr } = update;
            if (qr) {
                qrCodeData = qr;
            }

            if (connection === 'open') {
                qrCodeData = null;
                reconnectDelayMs = BOT_RECONNECT_BASE_MS;
                reconnectInProgress = false;
                if (reconnectTimer) {
                    clearTimeout(reconnectTimer);
                    reconnectTimer = null;
                }
                startConnectionIntervals(sock);
                startProactiveScheduler(sock);
                if (PROACTIVE_SEND_SHOWCASE_ON_START && startupShowcaseSentRunId !== runId) {
                    startupShowcaseSentRunId = runId;
                    setTimeout(async () => {
                        if (runId !== botRunId || activeSock !== sock) return;
                        try {
                            console.log('[PROACTIVO] Envío inmediato de showcase por reinicio.');
                            await sendPromptShowcase(sock);
                        } catch (error) {
                            console.error('[PROACTIVO] Error en showcase inmediato por reinicio:', error?.message || error);
                        }
                    }, 3000);
                }
                console.log('✅ BOT CONECTADO A WHATSAPP');
                processIncomingQueue(sock, runId).catch(() => {});
            }

            if (connection === 'close') {
                stopConnectionIntervals();
                const code = lastDisconnect?.error?.output?.statusCode;
                const reason = lastDisconnect?.error?.message || `code_${code || 'unknown'}`;
                const shouldReconnect = code !== DisconnectReason.loggedOut;
                if (shouldReconnect) {
                    scheduleReconnect(reason);
                } else {
                    reconnectInProgress = false;
                    console.log('Sesión cerrada. Elimina auth_info_baileys y vuelve a iniciar para escanear de nuevo.');
                }
            }
        });

        sock.ev.on('group-participants.update', async (event) => {
        if (runId !== botRunId) {
            return;
        }
        touchSocketActivity();
        if (event.action !== 'add') {
            return;
        }

        const groupJid = event.id;
        const validNewParticipants = [];

        for (const participantJid of event.participants) {
            try {
                if (isStorageReady) {
                    const record = await getModRecord(participantJid);
                    if (record?.isBanned) {
                        await upsertModRecord(participantJid, { $inc: { intentosReingreso: 1 } });
                        try {
                            await sock.groupParticipantsUpdate(groupJid, [participantJid], 'remove');
                        } catch (error) {
                            const failureText = isGroupPermissionError(error)
                                ? `🚨 ALERTA: El usuario baneado ${participantJid} intentó entrar al grupo ${groupJid}, pero necesito ser administrador para expulsarlo automáticamente. Usa .unban ${participantJid} si deseas perdonarlo.`
                                : `🚨 ALERTA: El usuario baneado ${participantJid} intentó entrar al grupo ${groupJid}, pero no pude expulsarlo automáticamente. Usa .unban ${participantJid} si deseas perdonarlo.`;
                            await sendPrivateAdminMessage(sock, failureText);
                            continue;
                        }
                        await sendPrivateAdminMessage(sock, `🚨 ALERTA: El usuario baneado ${participantJid} intentó entrar al grupo ${groupJid}. Fue expulsado automáticamente. Usa .unban ${participantJid} si deseas perdonarlo.`);
                        continue;
                    }
                }
                validNewParticipants.push(participantJid);
            } catch (error) {
                console.error('Error procesando ingreso al grupo:', error?.message || error);
            }
        }

        if (validNewParticipants.length === 0) {
            return;
        }

        if (!pendingWelcomesByGroup.has(groupJid)) {
            pendingWelcomesByGroup.set(groupJid, {
                participants: new Set(),
                timer: null
            });
        }

        const groupQueue = pendingWelcomesByGroup.get(groupJid);
        validNewParticipants.forEach((jid) => groupQueue.participants.add(jid));

        if (groupQueue.timer) {
            clearTimeout(groupQueue.timer);
        }

        try {
            await sock.sendPresenceUpdate('composing', groupJid);
        } catch (error) {
        }

        groupQueue.timer = setTimeout(async () => {
            const jidsToWelcome = Array.from(groupQueue.participants);
            pendingWelcomesByGroup.delete(groupJid);

            try {
                await sock.sendPresenceUpdate('paused', groupJid);
            } catch (error) {
            }

            if (jidsToWelcome.length > 0) {
                await sendBatchedWelcome(sock, groupJid, jidsToWelcome);
            }
        }, 5000);
    });

        sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (runId !== botRunId) {
            return;
        }
        touchSocketActivity();
        for (const msg of messages) {
            if (!msg?.key?.id || processedMessageIds.has(msg.key.id)) {
                continue;
            }
            processedMessageIds.add(msg.key.id);
            incomingQueue.push(msg);
        }
        if (incomingBufferTimeout) {
            clearTimeout(incomingBufferTimeout);
        }
        const bufferDelayMs = type === 'notify' ? 1200 : 600;
        incomingBufferTimeout = setTimeout(() => {
            incomingBufferTimeout = null;
            processIncomingQueue(sock, runId).catch(() => {});
        }, bufferDelayMs);
    });
    } finally {
        isStartingBot = false;
    }
}

startBot().catch((error) => {
    console.error('Error al iniciar bot:', error);
});
startKeepAlive();
startHealthWatchdog();
