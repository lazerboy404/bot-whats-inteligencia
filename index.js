const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers, downloadContentFromMessage, initAuthCreds, BufferJSON, proto, makeCacheableSignalKeyStore } = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;

const MONGO_URL = process.env.MONGO_URL || '';
const AUTH_STATE_COLLECTION = process.env.AUTH_STATE_COLLECTION || 'wa_auth_state';
const MOD_RECORDS_COLLECTION = process.env.MOD_RECORDS_COLLECTION || 'mod_records';
const BOT_CONFIG_COLLECTION = process.env.BOT_CONFIG_COLLECTION || 'bot_config';
const FORCE_LOCAL_STORAGE = ['1', 'true', 'yes', 'on'].includes(String(process.env.FORCE_LOCAL_STORAGE || '').toLowerCase());
const LOCAL_STORAGE_ENABLED = FORCE_LOCAL_STORAGE || !MONGO_URL;
const LOCAL_STORE_FILE = process.env.LOCAL_STORE_FILE || path.join(process.cwd(), 'data', 'castor_store.json');
const ADMIN_PHONE = process.env.ADMIN_PHONE || '5215564132674';
const ADMIN_NUMBER_VARIANTS = new Set(['5564132674', '525564132674', '5215564132674']);
const reportCooldownByUser = new Map();
const reportReferenceMap = new Map();
let ModRecordModel = null;
let AuthStateModel = null;
let BotConfigModel = null;
let isMongoReady = false;
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
let reconnectDelayMs = 4000;
let processErrorGuardReady = false;
let activeSock = null;
let isStartingBot = false;
let botRunId = 0;
let sessionResetDoneThisBoot = false;
let lastSocketActivityAt = Date.now();
let lastCommandHandledAt = 0;
const processedMessageIds = new Set();
const incomingQueue = [];
let isProcessingIncoming = false;
let incomingBufferTimeout = null;
const CASTOR_EMOJI = '🦫';
const CASTOR_DEFAULT_IMAGE_URL = process.env.CASTOR_DEFAULT_IMAGE_URL || 'https://raw.githubusercontent.com/lazerboy404/bot-whats-inteligencia/main/bienvenida.png';
const CASTOR_SEAL_STICKER_URL = process.env.CASTOR_SEAL_STICKER_URL || '';
const CASTOR_VALID_COMMANDS = new Set(['.reportar', '.advertir', '.ban', '.unban', '.sticker', '.fantasmas', '.cerrar', '.abrir', '.ping', '.top', '.random', '.comandos', '.reglas', '.miid', '.setadmin']);
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

setInterval(() => {
    processedMessageIds.clear();
}, 60 * 60 * 1000);

function cleanDigits(value) {
    return String(value || '').replace(/\D/g, '');
}

function getDefaultLocalStore() {
    return {
        modRecords: {},
        botConfig: {
            adminPrivateJid: '',
            adminSenderJid: '',
            updatedAt: null
        }
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
    if (!isMongoReady) {
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
    if (!isMongoReady) {
        return null;
    }
    if (LOCAL_STORAGE_ENABLED) {
        const store = ensureLocalStoreLoaded();
        return store.botConfig ? { ...store.botConfig } : null;
    }
    if (!BotConfigModel) {
        return null;
    }
    return BotConfigModel.findById('main').lean();
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
        '.top → muestra los usuarios más activos del grupo',
        '',
        '.random → menciona alguien al azar',
        '',
        '.comandos → muestra la lista de comandos para usuarios',
        '',
        '.reglas → muestra las reglas del grupo',
        '',
        getCastorSignatureText()
    ].join('\n');
}

async function ensureMongo() {
    if (isMongoReady) {
        return true;
    }
    if (LOCAL_STORAGE_ENABLED) {
        ensureLocalStoreLoaded();
        isMongoReady = true;
        console.log(`Almacenamiento local activado en ${LOCAL_STORE_FILE}`);
        return true;
    }
    if (!MONGO_URL) {
        console.error('MONGO_URL no está configurado. Moderación desactivada.');
        return false;
    }
    if (mongoose.connection.readyState === 0) {
        await mongoose.connect(MONGO_URL, { serverSelectionTimeoutMS: 5000 });
    }

    if (!mongoose.models.ModRecord) {
        const schema = new mongoose.Schema({
            userId: { type: String, required: true, unique: true, index: true },
            advertencias: { type: Number, default: 0 },
            motivos: { type: [String], default: [] },
            isBanned: { type: Boolean, default: false },
            intentosReingreso: { type: Number, default: 0 },
            actividadMensajes: { type: Number, default: 0 },
            ultimaActividad: { type: Date, default: null },
            countryOverride: { type: String, default: '' },
            flagOverride: { type: String, default: '' }
        });
        schema.index({ userId: 1 });
        schema.index({ ultimaActividad: 1 });
        ModRecordModel = mongoose.model('ModRecord', schema, MOD_RECORDS_COLLECTION);
    } else {
        ModRecordModel = mongoose.model('ModRecord');
    }

    if (!mongoose.models.AuthState) {
        const authSchema = new mongoose.Schema({
            _id: String,
            data: String
        });
        AuthStateModel = mongoose.model('AuthState', authSchema, AUTH_STATE_COLLECTION);
    } else {
        AuthStateModel = mongoose.model('AuthState');
    }

    if (!mongoose.models.BotConfig) {
        const botConfigSchema = new mongoose.Schema({
            _id: String,
            adminPrivateJid: { type: String, default: '' },
            adminSenderJid: { type: String, default: '' },
            updatedAt: { type: Date, default: Date.now }
        });
        BotConfigModel = mongoose.model('BotConfig', botConfigSchema, BOT_CONFIG_COLLECTION);
    } else {
        BotConfigModel = mongoose.model('BotConfig');
    }

    isMongoReady = true;
    return true;
}

async function useMongoAuthState() {
    const writeData = async (data, id) => {
        await AuthStateModel.updateOne(
            { _id: id },
            { $set: { data: JSON.stringify(data, BufferJSON.replacer) } },
            { upsert: true }
        );
    };

    const readData = async (id) => {
        const result = await AuthStateModel.findById(id).lean();
        if (!result?.data) {
            return null;
        }
        return JSON.parse(result.data, BufferJSON.reviver);
    };

    const removeData = async (id) => {
        await AuthStateModel.deleteOne({ _id: id });
    };

    const creds = await readData('creds') || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async (id) => {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value);
                        }
                        if (value) {
                            data[id] = value;
                        }
                    }));
                    return data;
                },
                set: async (newData) => {
                    const tasks = [];
                    for (const category of Object.keys(newData)) {
                        for (const id of Object.keys(newData[category])) {
                            const value = newData[category][id];
                            const key = `${category}-${id}`;
                            if (value) {
                                tasks.push(writeData(value, key));
                            } else {
                                tasks.push(removeData(key));
                            }
                        }
                    }
                    await Promise.all(tasks);
                }
            }
        },
        saveCreds: async () => writeData(creds, 'creds')
    };
}

async function saveBotConfigRecord(update) {
    if (LOCAL_STORAGE_ENABLED) {
        const store = ensureLocalStoreLoaded();
        store.botConfig = applyUpdateObject(store.botConfig || {}, update);
        saveLocalStore();
        return store.botConfig;
    }
    await BotConfigModel.updateOne({ _id: 'main' }, update, { upsert: true });
    return getSavedAdminConfig();
}

async function findModRecordsByUserIds(userIds) {
    if (LOCAL_STORAGE_ENABLED) {
        const store = ensureLocalStoreLoaded();
        return userIds
            .map((userId) => ({ userId, ...(store.modRecords?.[userId] || {}) }))
            .filter((record) => Object.keys(record).length > 1);
    }
    return ModRecordModel.find(
        { userId: { $in: userIds } },
        { userId: 1, actividadMensajes: 1, ultimaActividad: 1 }
    ).lean();
}

async function upsertModRecord(userId, update) {
    if (LOCAL_STORAGE_ENABLED) {
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
            flagOverride: ''
        };
        const next = applyUpdateObject(current, update);
        next.userId = userId;
        store.modRecords[userId] = next;
        saveLocalStore();
        return { ...next };
    }
    return ModRecordModel.findOneAndUpdate(
        { userId },
        update,
        { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
    );
}

async function getModRecord(userId) {
    if (LOCAL_STORAGE_ENABLED) {
        const store = ensureLocalStoreLoaded();
        return store.modRecords?.[userId] ? { ...store.modRecords[userId] } : null;
    }
    return ModRecordModel.findOne({ userId }).lean();
}

function touchLastActivityAsync(userId) {
    if (!isMongoReady || !userId) {
        return;
    }
    upsertModRecord(userId, {
        $set: { ultimaActividad: new Date() },
        $inc: { actividadMensajes: 1 }
    }).catch(() => {});
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
    if (isMongoReady) {
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
        getRulesText()
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
        getRulesText()
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
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: MongoDB no está conectado.' }, { quoted: msg });
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
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: MongoDB no está conectado.' }, { quoted: msg });
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
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: MongoDB no está conectado.' }, { quoted: msg });
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
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Moderación no disponible: MongoDB no está conectado.' }, { quoted: msg });
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
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: '⚠️ El ranking de actividad necesita MongoDB conectado.' }, { quoted: msg });
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
                actividadMensajes: Number(record.actividadMensajes || 0),
                ultimaActividad: record.ultimaActividad ? new Date(record.ultimaActividad).getTime() : 0
            };
        })
        .filter((entry) => entry && entry.actividadMensajes > 0)
        .sort((a, b) => {
            if (b.actividadMensajes !== a.actividadMensajes) {
                return b.actividadMensajes - a.actividadMensajes;
            }
            return b.ultimaActividad - a.ultimaActividad;
        })
        .slice(0, 10);

    if (!topEntries.length) {
        await sock.sendMessage(remoteJid, { text: '📊 Aún no hay suficiente actividad registrada para mostrar el top.' }, { quoted: msg });
        return;
    }

    const lines = topEntries.map((entry, index) => `${index + 1}. @${entry.mentionLabel} (${entry.displayName}) — ${entry.actividadMensajes} mensajes`);
    await sock.sendMessage(remoteJid, {
        text: `📊 Top de usuarios más activos\n\n${lines.join('\n')}`,
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
    if (!isMongoReady || !BotConfigModel) {
        await sock.sendMessage(remoteJid, { text: 'No pude guardar el chat admin porque MongoDB no está conectado.' }, { quoted: msg });
        return;
    }
    await saveAdminPrivateJids(privateJid.endsWith('@g.us') ? '' : privateJid, senderJid);
    await sock.sendMessage(remoteJid, {
        text: `✅ Admin vinculado.\nChat: ${privateJid}\nSender: ${senderJid}`
    }, { quoted: msg });
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
    if (msg.message.protocolMessage || msg.message?.reactionMessage) {
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
    try {
        await sock.readMessages([msg.key]);
    } catch (error) {
    }

    if (remoteJid.endsWith('@g.us') && text && hasGroupInviteLink(text) && !senderIsAdmin) {
        try {
            await sock.sendMessage(remoteJid, { delete: msg.key });
        } catch (error) {
        }
        if (isMongoReady) {
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
    if (reconnectTimer) {
        return;
    }
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
        startBot().catch((error) => {
            console.error('Error al reintentar inicio del bot:', error?.message || error);
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
        try {
        await ensureMongo();
        } catch (error) {
        console.error('No se pudo conectar MongoDB al iniciar:', error?.message || error);
        }
    try {
        if (activeSock) {
            try {
                activeSock.end(new Error('new_run_started'));
            } catch (error) {
            }
        }

        if (RESET_WA_SESSION_ON_BOOT && !sessionResetDoneThisBoot) {
            if (isMongoReady && AuthStateModel) {
                await AuthStateModel.deleteMany({});
                console.log('RESET_WA_SESSION_ON_BOOT activo: sesión de Mongo reiniciada.');
            }
            try {
                fs.rmSync('auth_info_baileys', { recursive: true, force: true });
                console.log('RESET_WA_SESSION_ON_BOOT activo: auth_info_baileys eliminado.');
            } catch (error) {
            }
            sessionResetDoneThisBoot = true;
        }

        let state;
        let saveCreds;
        if (isMongoReady && AuthStateModel) {
            try {
                const auth = await useMongoAuthState();
                state = auth.state;
                saveCreds = auth.saveCreds;
                console.log('Sesión de WhatsApp usando MongoDB Atlas');
            } catch (error) {
                const auth = await useMultiFileAuthState('auth_info_baileys');
                state = auth.state;
                saveCreds = auth.saveCreds;
                console.log('Sesión de WhatsApp usando archivos locales por fallback');
            }
        } else {
            const auth = await useMultiFileAuthState('auth_info_baileys');
            state = auth.state;
            saveCreds = auth.saveCreds;
            console.log('Sesión de WhatsApp usando archivos locales');
        }

        const { version } = await fetchLatestBaileysVersion();

        const sock = makeWASocket({
            version,
            logger: pino({ level: 'info' }),
            printQRInTerminal: true,
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
                startConnectionIntervals(sock);
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
                if (isMongoReady) {
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
