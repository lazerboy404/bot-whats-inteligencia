const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers, downloadContentFromMessage, initAuthCreds, BufferJSON, proto } = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const mongoose = require('mongoose');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;

const MONGO_URL = process.env.MONGO_URL || '';
const ADMIN_PHONE = '5564132674';
const ADMIN_NUMBER_VARIANTS = new Set(['5564132674', '525564132674', '5215564132674']);
const reportCooldownByUser = new Map();
const reportReferenceMap = new Map();
let ModRecordModel = null;
let AuthStateModel = null;
let TroncoUserModel = null;
let TroncoMessageModel = null;
let DiqueGroupModel = null;
let EventoGroupModel = null;
let isMongoReady = false;
const GROUP_INVITE_REGEX = /(chat\.whatsapp\.com\/[a-zA-Z0-9]{20,}|wa\.me\/joinlink\/)/i;
let keepAliveInterval = null;
const SEND_MIN_DELAY_MS = Number(process.env.SEND_MIN_DELAY_MS || 600);
const SEND_MAX_DELAY_MS = Number(process.env.SEND_MAX_DELAY_MS || 1800);
const PER_CHAT_MIN_GAP_MS = Number(process.env.PER_CHAT_MIN_GAP_MS || 1400);
const KEEP_ALIVE_INTERVAL_MS = Number(process.env.KEEP_ALIVE_INTERVAL_MS || (10 * 60 * 1000));
const lastSentAtByJid = new Map();
let globalSendQueue = Promise.resolve();
const closeTimersByGroup = new Map();
let reconnectTimer = null;
let reconnectDelayMs = 4000;
let processErrorGuardReady = false;
const CASTOR_EMOJI = '🦫';
const CASTOR_DEFAULT_IMAGE_URL = process.env.CASTOR_DEFAULT_IMAGE_URL || 'https://raw.githubusercontent.com/lazerboy404/bot-whats-inteligencia/main/bienvenida.png';
const CASTOR_SEAL_STICKER_URL = process.env.CASTOR_SEAL_STICKER_URL || '';
const CASTOR_VALID_COMMANDS = new Set(['.reporte', '.reportar', '.advertir', '.unban', '.sticker', '.fantasmas', '.cerrar', '.abrir', '.pais', '.troncos', '.ranking', '.dique', '.perfil', '.destacar', '.evento', '.debatir']);
const TRONCOS_AUTO_LIKE_THRESHOLD_1 = Number(process.env.TRONCOS_AUTO_LIKE_THRESHOLD_1 || 5);
const TRONCOS_AUTO_LIKE_THRESHOLD_2 = Number(process.env.TRONCOS_AUTO_LIKE_THRESHOLD_2 || 10);
const TRONCOS_DAILY_AUTO_LIMIT = Number(process.env.TRONCOS_DAILY_AUTO_LIMIT || 5);
const DIQUE_LEVELS = [100, 300, 700];
const BAILEYS_QUERY_TIMEOUT_MS = Number(process.env.BAILEYS_QUERY_TIMEOUT_MS || 60000);
const BAILEYS_CONNECT_TIMEOUT_MS = Number(process.env.BAILEYS_CONNECT_TIMEOUT_MS || 60000);
const BAILEYS_KEEPALIVE_MS = Number(process.env.BAILEYS_KEEPALIVE_MS || 30000);
const BOT_RECONNECT_BASE_MS = Number(process.env.BOT_RECONNECT_BASE_MS || 4000);
const BOT_RECONNECT_MAX_MS = Number(process.env.BOT_RECONNECT_MAX_MS || 45000);
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-1.5-flash';
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

function cleanDigits(value) {
    return String(value || '').replace(/\D/g, '');
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
    if (base.length === 10) {
        return toJid(`52${base}`);
    }
    return toJid(base);
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
    if (!/(dique|estanque|presa|corriente)/i.test(text)) {
        text = `${text}\n\n${CASTOR_EMOJI} Castor Bot: Estamos reforzando la presa en el dique.`;
    }
    return text;
}

function sanitizeDebateText(value) {
    return String(value || '')
        .replace(/https?:\/\/\S+|www\.\S+/gi, ' ')
        .replace(/([\p{Extended_Pictographic}])\1{1,}/gu, '$1')
        .replace(/(.)\1{5,}/g, '$1$1')
        .replace(/\s+/g, ' ')
        .trim();
}

function normalizeDebateAiOutput(value) {
    const raw = String(value || '').replace(/\r/g, '').trim();
    if (!raw) {
        return '';
    }
    const lines = raw.split('\n').map((line) => line.trim()).filter((line, idx, arr) => line || (idx > 0 && arr[idx - 1]));
    return lines.join('\n');
}

function getFallbackDebateText() {
    return [
        '🦫 El castor analizó el debate...',
        '',
        '📌 Tema: insuficiente contexto',
        '',
        '💡 Conclusión: no hay suficiente información para decidir'
    ].join('\n');
}

function getDebatePrompt(conversationText) {
    return [
        'Eres un moderador imparcial representado como un castor inteligente y justo dentro de un grupo.',
        '',
        'Analiza los siguientes mensajes de una discusión:',
        '',
        conversationText,
        '',
        'Tu tarea:',
        '1. Resume el tema del desacuerdo en una sola línea clara',
        '2. Identifica quién tiene el argumento más sólido (si aplica)',
        '3. Señala un error o debilidad importante (si existe)',
        '4. Da una conclusión clara y justa',
        '',
        'Reglas:',
        '- Sé neutral, objetivo y justo',
        '- No insultes ni ataques a ningún usuario',
        '- No inventes información',
        '- Si no hay suficiente contexto, indícalo',
        '- Mantén la respuesta breve (máx. 4–5 líneas)',
        '',
        'Formato de respuesta obligatorio:',
        '',
        '🦫 El castor analizó el debate...',
        '',
        '📌 Tema: [resumen corto]',
        '',
        '✅ Mejor argumento: @usuario',
        '⚠️ Detalle: [error o punto débil]',
        '',
        '💡 Conclusión: [resultado claro y neutral]'
    ].join('\n');
}

function collectDebateMessages(msg) {
    const maxCollect = 7;
    const result = [];
    const text = sanitizeDebateText(extractTextFromMessage(msg.message));
    if (text) {
        result.push({
            userId: msg.key.participant || msg.key.remoteJid,
            text
        });
    }

    let current = msg.message;
    let guard = 0;
    while (guard < maxCollect) {
        guard += 1;
        const quoted = getQuotedPayload(current);
        if (!quoted?.quotedMessage || !quoted?.quotedParticipant) {
            break;
        }
        const quotedText = sanitizeDebateText(extractTextFromMessage(quoted.quotedMessage));
        if (quotedText) {
            result.push({
                userId: quoted.quotedParticipant,
                text: quotedText
            });
        }
        current = quoted.quotedMessage;
    }
    return result.reverse();
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
    if (arg.includes('@s.whatsapp.net')) {
        return arg;
    }
    const digits = cleanDigits(arg);
    if (digits.length >= 8) {
        return toJid(digits);
    }
    return null;
}

function parseTargetFromReportText(text) {
    const match = String(text || '').match(/ID infractor:\s*([0-9]+@s\.whatsapp\.net)/i);
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

function getRulesText() {
    return [
        '✋Reglas del Grupo⚠️:',
        '',
        '🚫 *Prohibido:*',
        'Contenido sexual, erótico o +18 (incluye IA).',
        '',
        '🤝 *Normas:*',
        '• Respeto entre todos',
        '• Sin insultos ni acoso',
        '• No spam ni cadenas'
    ].join('\n');
}

function getUserCommandsText() {
    return [
        '🛠️ *Comandos Disponibles*',
        '',
        '• .sticker → crear sticker (responder a imagen)',
        '• .troncos → ver tus 🪵',
        '• .ranking → top usuarios 🏆',
        '• .dique → progreso del grupo 🧱',
        '• .reportar → reportar un usuario que pudo romper las reglas (revisión por admin, 3 faltas = ban)'
    ].join('\n');
}

function getTroncosDynamicsText() {
    return [
        '🪵 *Troncos (dinámica)*',
        'Los troncos 🪵 son recompensas que ganas por aportar contenido de calidad en el grupo.',
        '',
        '• 10 reacciones positivas = +1 🪵',
        '• 20 reacciones positivas = +2 🪵',
        '',
        '🏆 Sirven para ranking y construir el dique del grupo.',
        '💡 Entre más calidad, más reacciones → más 🪵'
    ].join('\n');
}

async function ensureMongo() {
    if (isMongoReady) {
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
            ultimaActividad: { type: Date, default: null },
            countryOverride: { type: String, default: '' },
            flagOverride: { type: String, default: '' }
        });
        schema.index({ userId: 1 });
        schema.index({ ultimaActividad: 1 });
        ModRecordModel = mongoose.model('ModRecord', schema, 'mod_records');
    } else {
        ModRecordModel = mongoose.model('ModRecord');
    }

    if (!mongoose.models.AuthState) {
        const authSchema = new mongoose.Schema({
            _id: String,
            data: String
        });
        AuthStateModel = mongoose.model('AuthState', authSchema, 'wa_auth_state');
    } else {
        AuthStateModel = mongoose.model('AuthState');
    }

    if (!mongoose.models.TroncoUser) {
        const troncoUserSchema = new mongoose.Schema({
            groupJid: { type: String, required: true, index: true },
            userId: { type: String, required: true, index: true },
            troncos: { type: Number, default: 0 },
            autoTodayDate: { type: String, default: '' },
            autoTodayEarned: { type: Number, default: 0 }
        });
        troncoUserSchema.index({ groupJid: 1, userId: 1 }, { unique: true });
        TroncoUserModel = mongoose.model('TroncoUser', troncoUserSchema, 'beaver_troncos_users');
    } else {
        TroncoUserModel = mongoose.model('TroncoUser');
    }

    if (!mongoose.models.TroncoMessage) {
        const troncoMessageSchema = new mongoose.Schema({
            groupJid: { type: String, required: true, index: true },
            messageId: { type: String, required: true, index: true },
            senderId: { type: String, required: true },
            reactors: { type: [String], default: [] },
            troncosAwarded: { type: Number, default: 0 },
            awardedAt: { type: Date, default: null }
        });
        troncoMessageSchema.index({ groupJid: 1, messageId: 1 }, { unique: true });
        TroncoMessageModel = mongoose.model('TroncoMessage', troncoMessageSchema, 'beaver_troncos_messages');
    } else {
        TroncoMessageModel = mongoose.model('TroncoMessage');
    }

    if (!mongoose.models.DiqueGroup) {
        const diqueSchema = new mongoose.Schema({
            groupJid: { type: String, required: true, unique: true, index: true },
            totalTroncos: { type: Number, default: 0 }
        });
        DiqueGroupModel = mongoose.model('DiqueGroup', diqueSchema, 'beaver_diques');
    } else {
        DiqueGroupModel = mongoose.model('DiqueGroup');
    }

    if (!mongoose.models.EventoGroup) {
        const eventoSchema = new mongoose.Schema({
            groupJid: { type: String, required: true, unique: true, index: true },
            isActive: { type: Boolean, default: false },
            title: { type: String, default: '' },
            rewardParticipation: { type: Number, default: 1 },
            rewardWin: { type: Number, default: 2 }
        });
        EventoGroupModel = mongoose.model('EventoGroup', eventoSchema, 'beaver_eventos');
    } else {
        EventoGroupModel = mongoose.model('EventoGroup');
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

async function upsertModRecord(userId, update) {
    return ModRecordModel.findOneAndUpdate(
        { userId },
        update,
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );
}

async function getModRecord(userId) {
    return ModRecordModel.findOne({ userId }).lean();
}

function touchLastActivityAsync(userId) {
    if (!isMongoReady || !userId) {
        return;
    }
    upsertModRecord(userId, { $set: { ultimaActividad: new Date() } }).catch(() => {});
}

function getTodayKey() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
}

function getRangoByTroncos(troncos) {
    if (troncos >= 60) return 'Maestro del Dique';
    if (troncos >= 20) return 'Constructor';
    return 'Novato';
}

function jidToDisplayName(userId) {
    const num = getNumberFromJid(userId);
    return num ? `@${num}` : sanitizeText(userId, 60);
}

async function ensureTroncoUser(groupJid, userId) {
    return TroncoUserModel.findOneAndUpdate(
        { groupJid, userId },
        { $setOnInsert: { troncos: 0, autoTodayDate: '', autoTodayEarned: 0 } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );
}

async function addTroncos(groupJid, userId, amount, reason, options = {}) {
    if (!isMongoReady || !groupJid || !userId || amount <= 0) {
        return { credited: 0, totalUser: 0, totalGroup: 0 };
    }
    const respectDailyLimit = !!options.respectDailyLimit;
    const todayKey = getTodayKey();
    const user = await ensureTroncoUser(groupJid, userId);

    let credited = amount;
    if (respectDailyLimit) {
        const todayEarned = user.autoTodayDate === todayKey ? user.autoTodayEarned : 0;
        const remaining = Math.max(0, TRONCOS_DAILY_AUTO_LIMIT - todayEarned);
        credited = Math.min(amount, remaining);
    }
    if (credited <= 0) {
        return { credited: 0, totalUser: user.troncos, totalGroup: 0 };
    }

    const update = { $inc: { troncos: credited } };
    if (respectDailyLimit) {
        if (user.autoTodayDate !== todayKey) {
            update.$set = { autoTodayDate: todayKey, autoTodayEarned: credited };
        } else {
            update.$inc.autoTodayEarned = credited;
        }
    }
    const updatedUser = await TroncoUserModel.findOneAndUpdate(
        { groupJid, userId },
        update,
        { new: true }
    );

    const updatedGroup = await DiqueGroupModel.findOneAndUpdate(
        { groupJid },
        { $inc: { totalTroncos: credited } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );

    return {
        credited,
        totalUser: updatedUser?.troncos || 0,
        totalGroup: updatedGroup?.totalTroncos || 0,
        reason: sanitizeText(reason, 80)
    };
}

async function getDiqueStats(groupJid) {
    const group = await DiqueGroupModel.findOneAndUpdate(
        { groupJid },
        { $setOnInsert: { totalTroncos: 0 } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );
    const total = group?.totalTroncos || 0;
    let level = 0;
    for (const threshold of DIQUE_LEVELS) {
        if (total >= threshold) level += 1;
    }
    const nextThreshold = DIQUE_LEVELS.find((v) => total < v) || null;
    return { total, level, nextThreshold };
}

async function handleReactionReward(sock, msg, remoteJid) {
    if (!isMongoReady || !remoteJid.endsWith('@g.us')) {
        return;
    }
    const reactionMessage = msg.message?.reactionMessage;
    const targetKey = reactionMessage?.key;
    const reactionText = sanitizeText(reactionMessage?.text || '', 8);
    if (!targetKey?.id || !reactionText) {
        return;
    }

    const reactorJid = msg.key.participant || msg.key.remoteJid;
    if (!reactorJid || reactorJid === targetKey.participant || reactorJid === sock.user?.id) {
        return;
    }

    const senderId = targetKey.participant || targetKey.remoteJid;
    if (!senderId) {
        return;
    }

    const record = await TroncoMessageModel.findOneAndUpdate(
        { groupJid: remoteJid, messageId: targetKey.id },
        {
            $setOnInsert: { senderId, troncosAwarded: 0, reactors: [] },
            $addToSet: { reactors: reactorJid }
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );

    if (!record || record.troncosAwarded > 0) {
        return;
    }

    const validReactors = (record.reactors || []).filter((jid) => jid !== senderId && jid !== sock.user?.id);
    const likes = validReactors.length;
    let reward = 0;
    if (likes >= TRONCOS_AUTO_LIKE_THRESHOLD_2) {
        reward = 2;
    } else if (likes >= TRONCOS_AUTO_LIKE_THRESHOLD_1) {
        reward = 1;
    }
    if (!reward) {
        return;
    }

    const lock = await TroncoMessageModel.findOneAndUpdate(
        { groupJid: remoteJid, messageId: targetKey.id, troncosAwarded: 0 },
        { $set: { troncosAwarded: reward, awardedAt: new Date() } },
        { new: true }
    );
    if (!lock) {
        return;
    }

    const added = await addTroncos(remoteJid, senderId, reward, 'calidad_por_reacciones', { respectDailyLimit: true });
    if (!added.credited) {
        return;
    }

    const mention = jidToDisplayName(senderId);
    await sock.sendMessage(remoteJid, {
        text: `¡Misión Dique Cumplida! ${mention} ganó 🪵 +${added.credited} por contenido de calidad (${likes} reacciones).`,
        mentions: mention.startsWith('@') ? [senderId] : []
    });
}

async function handleTroncosCommand(sock, msg, remoteJid) {
    if (!isMongoReady || !remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB y un grupo para revisar troncos.' }, { quoted: msg });
        return;
    }
    const userId = msg.key.participant || msg.key.remoteJid;
    const user = await ensureTroncoUser(remoteJid, userId);
    const rango = getRangoByTroncos(user.troncos || 0);
    await sock.sendMessage(remoteJid, { text: `Tienes 🪵 ${user.troncos || 0}. Rango actual: ${rango}.` }, { quoted: msg });
}

async function handleRankingCommand(sock, msg, remoteJid) {
    if (!isMongoReady || !remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB y un grupo para mostrar ranking.' }, { quoted: msg });
        return;
    }
    const top = await TroncoUserModel.find({ groupJid: remoteJid }).sort({ troncos: -1 }).limit(10).lean();
    if (!top.length) {
        await sock.sendMessage(remoteJid, { text: 'Aún no hay troncos registrados en el estanque.' }, { quoted: msg });
        return;
    }
    const lines = top.map((u, idx) => `${idx + 1}. ${jidToDisplayName(u.userId)} — 🪵 ${u.troncos || 0}`);
    await sock.sendMessage(remoteJid, { text: `🏆 Ranking del dique\n${lines.join('\n')}` }, { quoted: msg });
}

async function handleDiqueCommand(sock, msg, remoteJid) {
    if (!isMongoReady || !remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB y un grupo para mostrar progreso del dique.' }, { quoted: msg });
        return;
    }
    const stats = await getDiqueStats(remoteJid);
    const next = stats.nextThreshold ? `${stats.nextThreshold - stats.total} troncos para el siguiente nivel.` : 'El dique alcanzó el máximo nivel configurado.';
    await sock.sendMessage(remoteJid, { text: `🧱 Progreso del dique\nTotal colectivo: 🪵 ${stats.total}\nNivel actual: ${stats.level}\n${next}` }, { quoted: msg });
}

async function handlePerfilCommand(sock, msg, text, remoteJid) {
    if (!isMongoReady || !remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB y un grupo para mostrar perfil.' }, { quoted: msg });
        return;
    }
    const target = parseTargetFromTextOrMention(msg, text) || (msg.key.participant || msg.key.remoteJid);
    const user = await ensureTroncoUser(remoteJid, target);
    const stats = await getDiqueStats(remoteJid);
    const percent = stats.total > 0 ? ((user.troncos / stats.total) * 100).toFixed(1) : '0.0';
    const mention = jidToDisplayName(target);
    await sock.sendMessage(remoteJid, {
        text: `📦 Perfil del castor ${mention}\nTroncos: 🪵 ${user.troncos}\nRango: ${getRangoByTroncos(user.troncos)}\nAportación al dique: ${percent}%`,
        mentions: mention.startsWith('@') ? [target] : []
    }, { quoted: msg });
}

async function handleDestacarCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .destacar solo funciona en grupos.' }, { quoted: msg });
        return;
    }
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB para gestionar troncos.' }, { quoted: msg });
        return;
    }
    const target = parseTargetFromTextOrMention(msg, text);
    if (!target) {
        await sock.sendMessage(remoteJid, { text: 'Uso: .destacar @usuario [2|3]' }, { quoted: msg });
        return;
    }
    const amount = text.includes(' 3') ? 3 : 2;
    const added = await addTroncos(remoteJid, target, amount, 'destacado_admin');
    await sendCastorSealSticker(sock, remoteJid, msg);
    await sock.sendMessage(remoteJid, {
        text: `Contenido destacado. ${jidToDisplayName(target)} recibe 🪵 +${added.credited}.`,
        mentions: [target]
    }, { quoted: msg });
}

async function handleEventoCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .evento solo funciona en grupos.' }, { quoted: msg });
        return;
    }
    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }
    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Necesito MongoDB para gestionar eventos.' }, { quoted: msg });
        return;
    }

    const parts = sanitizeText(text).split(/\s+/);
    const sub = (parts[1] || '').toLowerCase();
    const target = parseTargetFromTextOrMention(msg, text);

    if (sub === 'on' || sub === 'iniciar') {
        const title = sanitizeText(parts.slice(2).join(' '), 80) || 'Dinámica activa';
        await EventoGroupModel.findOneAndUpdate(
            { groupJid: remoteJid },
            { $set: { isActive: true, title } },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        );
        await sock.sendMessage(remoteJid, { text: `🎯 Evento activado: ${title}. Usa .evento participar @usuario o .evento ganar @usuario.` }, { quoted: msg });
        return;
    }

    if (sub === 'off' || sub === 'cerrar') {
        await EventoGroupModel.findOneAndUpdate(
            { groupJid: remoteJid },
            { $set: { isActive: false } },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        );
        await sock.sendMessage(remoteJid, { text: '🎯 Evento desactivado en el estanque.' }, { quoted: msg });
        return;
    }

    const eventState = await EventoGroupModel.findOneAndUpdate(
        { groupJid: remoteJid },
        { $setOnInsert: { isActive: false, title: '', rewardParticipation: 1, rewardWin: 2 } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );

    if (sub === 'estado' || !sub) {
        const status = eventState.isActive ? `Activo: ${eventState.title}` : 'Inactivo';
        await sock.sendMessage(remoteJid, { text: `🎯 Estado del evento: ${status}.` }, { quoted: msg });
        return;
    }

    if (!eventState.isActive) {
        await sock.sendMessage(remoteJid, { text: 'No hay evento activo. Usa .evento on [titulo].' }, { quoted: msg });
        return;
    }

    if (!target) {
        await sock.sendMessage(remoteJid, { text: 'Debes mencionar a un usuario para premiar en el evento.' }, { quoted: msg });
        return;
    }

    let amount = 0;
    if (sub === 'participar') amount = eventState.rewardParticipation || 1;
    if (sub === 'ganar') amount = eventState.rewardWin || 2;
    if (!amount) {
        await sock.sendMessage(remoteJid, { text: 'Subcomando no válido. Usa .evento participar @usuario o .evento ganar @usuario.' }, { quoted: msg });
        return;
    }

    const added = await addTroncos(remoteJid, target, amount, `evento_${sub}`);
    await sendCastorSealSticker(sock, remoteJid, msg);
    await sock.sendMessage(remoteJid, {
        text: `Evento registrado. ${jidToDisplayName(target)} gana 🪵 +${added.credited}.`,
        mentions: [target]
    }, { quoted: msg });
}

async function sendPrivateAdminMessage(sock, text) {
    const adminJid = getAdminJid();
    await sock.sendMessage(adminJid, { text: sanitizeText(text, 9000) });
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

async function handleSetCountryCommand(sock, msg, text, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: 'El comando .pais solo funciona en grupos.' }, { quoted: msg });
        return;
    }

    const isAuthorized = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
    if (!isAuthorized) {
        await sock.sendMessage(remoteJid, { text: 'Acceso denegado. Solo administradores.' }, { quoted: msg });
        return;
    }

    if (!isMongoReady) {
        await sock.sendMessage(remoteJid, { text: 'Para usar .pais necesito MongoDB conectado.' }, { quoted: msg });
        return;
    }

    const quoted = getQuotedPayload(msg.message);
    const targetJid = parseTargetFromTextOrMention(msg, text) || quoted?.quotedParticipant;
    if (!targetJid) {
        await sock.sendMessage(remoteJid, { text: 'Uso: .pais @usuario México 🇲🇽 o responde al mensaje del usuario con .pais México 🇲🇽' }, { quoted: msg });
        return;
    }

    const targetNumber = getNumberFromJid(targetJid);
    let payload = sanitizeText(text).replace(/^\.pais\s*/i, '').trim();
    if (targetNumber) {
        payload = payload.replace(new RegExp(`@?${targetNumber}`, 'g'), '').trim();
    }
    payload = payload.replace(targetJid, '').trim();

    if (!payload) {
        await sock.sendMessage(remoteJid, { text: 'Debes indicar país y opcional bandera. Ejemplo: .pais @usuario México 🇲🇽' }, { quoted: msg });
        return;
    }

    const indicatorChars = [...payload].filter((ch) => {
        const cp = ch.codePointAt(0);
        return cp >= 0x1F1E6 && cp <= 0x1F1FF;
    });
    const flag = indicatorChars.length >= 2 ? indicatorChars.slice(-2).join('') : '🌍';
    const country = payload.replace(flag, '').trim() || payload;

    await upsertModRecord(targetJid, {
        $set: {
            countryOverride: country,
            flagOverride: flag
        }
    });

    const mention = targetNumber ? `@${targetNumber}` : '@usuario';
    await sock.sendMessage(remoteJid, {
        text: `País manual guardado para ${mention}: ${country} ${flag}.`,
        mentions: targetNumber ? [targetJid] : []
    }, { quoted: msg });
}

async function sendWelcome(sock, groupJid, participantJid) {
    const number = getNumberFromJid(participantJid);
    const mention = number ? `@${number}` : '@usuario';
    const resolvedLocation = await resolveCountryAndFlag(sock, groupJid, participantJid);
    const country = resolvedLocation.country;
    const flag = resolvedLocation.flag;
    const welcomeText = [
        `${CASTOR_EMOJI} ¡Un nuevo castor ha llegado al estanque! Bienvenido/a ${mention}.`,
        `Nos saludas desde ${flag} ${country}. Soy Castor Bot, el guardián de este dique. ¡Ponte cómodo y ayudemos a construir!`,
        '',
        getRulesText(),
        '',
        getUserCommandsText(),
        '',
        getTroncosDynamicsText()
    ].join('\n');

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
            caption: welcomeText,
            mentions: number ? [participantJid] : []
        });
    } catch (error) {
        await sock.sendMessage(groupJid, {
            text: welcomeText,
            mentions: number ? [participantJid] : []
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
        await sock.sendMessage(remoteJid, { text: `Debes esperar ${wait}s para usar .reporte de nuevo.` }, { quoted: msg });
        return;
    }

    const quoted = getQuotedPayload(msg.message);
    if (!quoted?.quotedParticipant || !quoted?.quotedMessage) {
        await sock.sendMessage(remoteJid, { text: 'Para usar .reporte debes responder a un mensaje.' }, { quoted: msg });
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
    const motive = sanitizeText(text).split(/\s+/).slice(1).join(' ');
    const cleanMotive = motive || 'Sin motivo adicional.';

    const reportText = [
        '🧾 REPORTE FORENSE',
        `Grupo: ${remoteJid}`,
        `Reportante: ${reporterId}`,
        `ID infractor: ${offenderId}`,
        `Motivo: ${cleanMotive}`,
        `Tipo de contenido: ${detail.mediaType}`,
        `Texto citado: ${detail.text || '(sin texto visible)'}`,
        `Contenido crudo: ${detail.raw || '(sin contenido)'}`
    ].join('\n');

    const sentReport = await sock.sendMessage(getAdminJid(), { text: reportText });
    reportReferenceMap.set(sentReport.key.id, { offenderJid: offenderId, groupJid: remoteJid });
    await sock.sendMessage(getAdminJid(), { text: `.advertir ${offenderId}` });
    await sock.sendMessage(remoteJid, { text: '✅ Reporte recibido. Evidencia preservada y enviada a administración.' }, { quoted: msg });
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
        : `⛔ ${mention} alcanzó 3/3 advertencias y quedó marcado como baneado. No pude expulsarlo automáticamente.`;
    await sock.sendMessage(remoteJid, { text: resultText, mentions: [resolved.targetJid] }, { quoted: msg });
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
    if (!CASTOR_SEAL_STICKER_URL) {
        return;
    }
    try {
        await sock.sendMessage(remoteJid, { sticker: { url: CASTOR_SEAL_STICKER_URL } }, quotedMsg ? { quoted: quotedMsg } : undefined);
    } catch (error) {
    }
}

async function applyWarning(sock, targetJid, groupJid, reason) {
    const record = await upsertModRecord(targetJid, {
        $inc: { advertencias: 1 },
        $push: { motivos: sanitizeText(reason, 500) }
    });
    const warningCount = record.advertencias;
    let kicked = false;
    if (warningCount >= 3) {
        await upsertModRecord(targetJid, { $set: { isBanned: true } });
        if (groupJid) {
            try {
                await sock.groupParticipantsUpdate(groupJid, [targetJid], 'remove');
                kicked = true;
            } catch (error) {
                kicked = false;
            }
        }
    }
    return { warningCount, kicked };
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
    const records = await ModRecordModel.find(
        { userId: { $in: participantIds } },
        { userId: 1, ultimaActividad: 1 }
    ).lean();

    const recordsByUser = new Map(records.map((item) => [item.userId, item]));
    const inactive = [];
    for (const participant of participants) {
        const isAdmin = participant.admin === 'admin' || participant.admin === 'superadmin';
        if (isAdmin) {
            continue;
        }
        const rec = recordsByUser.get(participant.id);
        if (!rec?.ultimaActividad || new Date(rec.ultimaActividad) < cutoff) {
            const number = getNumberFromJid(participant.id);
            const name = sanitizeText(participant.notify || participant.name || participant.id, 120);
            const lastActivityText = rec?.ultimaActividad
                ? new Date(rec.ultimaActividad).toISOString().slice(0, 10)
                : 'sin registro';
            inactive.push(`• ${name} (@${number}) - última actividad: ${lastActivityText}`);
        }
    }

    const senderJid = msg.key.participant || msg.key.remoteJid;
    const summary = inactive.length > 0
        ? inactive.join('\n')
        : 'No se detectaron usuarios inactivos en ese rango.';
    const privateText = `👻 Reporte de fantasmas\nGrupo: ${metadata.subject}\nRango: ${days} días\nTotal inactivos: ${inactive.length}\n\n${summary}`;
    await sock.sendMessage(senderJid, { text: privateText });
    await sock.sendMessage(remoteJid, { text: '✅ Lista de inactivos enviada por privado al administrador.' }, { quoted: msg });
}

async function ensureBotIsAdmin(sock, remoteJid) {
    if (!sock?.user?.id) {
        return false;
    }
    return isGroupAdmin(sock, remoteJid, sock.user.id);
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

    const botIsAdmin = await ensureBotIsAdmin(sock, remoteJid);
    if (!botIsAdmin) {
        await sock.sendMessage(remoteJid, { text: 'Necesito ser administrador del grupo para cerrarlo.' }, { quoted: msg });
        return;
    }

    const rawDuration = sanitizeText(text).replace(/^\.cerrar\s*/i, '').trim();
    if (rawDuration) {
        const durationMs = parseCloseDurationMs(rawDuration);
        if (!durationMs) {
            await sock.sendMessage(remoteJid, { text: 'Formato inválido. Usa por ejemplo: .cerrar 20min o .cerrar 1 hora' }, { quoted: msg });
            return;
        }

        const activeTimer = closeTimersByGroup.get(remoteJid);
        if (activeTimer?.timer) {
            clearTimeout(activeTimer.timer);
        }

        await sock.groupSettingUpdate(remoteJid, 'announcement');
        const reopenAt = new Date(Date.now() + durationMs);
        const reopenAtLabel = reopenAt.toLocaleString('es-MX', { hour12: false });
        await sock.sendMessage(remoteJid, { text: `🔒 Grupo cerrado por ${rawDuration}. Se abrirá automáticamente el ${reopenAtLabel}.` }, { quoted: msg });

        const timer = setTimeout(async () => {
            try {
                await sock.groupSettingUpdate(remoteJid, 'not_announcement');
                await sock.sendMessage(remoteJid, { text: '🔓 El grupo se abrió automáticamente. Ya todos pueden enviar mensajes.' });
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

    await sock.groupSettingUpdate(remoteJid, 'announcement');
    await sock.sendMessage(remoteJid, { text: '🔒 Grupo cerrado hasta nuevo aviso. Solo administradores pueden enviar mensajes.' }, { quoted: msg });
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

    const botIsAdmin = await ensureBotIsAdmin(sock, remoteJid);
    if (!botIsAdmin) {
        await sock.sendMessage(remoteJid, { text: 'Necesito ser administrador del grupo para abrirlo.' }, { quoted: msg });
        return;
    }

    const activeTimer = closeTimersByGroup.get(remoteJid);
    if (activeTimer?.timer) {
        clearTimeout(activeTimer.timer);
        closeTimersByGroup.delete(remoteJid);
    }

    await sock.groupSettingUpdate(remoteJid, 'not_announcement');
    await sock.sendMessage(remoteJid, { text: '🔓 Grupo abierto. Todos los miembros ya pueden enviar mensajes.' }, { quoted: msg });
}

async function handleDebatirCommand(sock, msg, remoteJid) {
    if (!remoteJid.endsWith('@g.us')) {
        await sock.sendMessage(remoteJid, { text: '🦫 Este comando solo funciona en grupos' }, { quoted: msg });
        return;
    }

    const collected = collectDebateMessages(msg);
    if (collected.length < 2) {
        await sock.sendMessage(remoteJid, { text: '🦫 Selecciona varios mensajes para debatir' }, { quoted: msg });
        return;
    }

    if (collected.length > 5) {
        await sock.sendMessage(remoteJid, { text: '🦫 Selecciona máximo 5 mensajes' }, { quoted: msg });
        return;
    }

    const uniqueUsers = new Set(collected.map((m) => m.userId)).size;
    if (uniqueUsers < 2) {
        await sock.sendMessage(remoteJid, { text: '🦫 No hay un debate claro aquí' }, { quoted: msg });
        return;
    }

    const lines = collected
        .map((entry) => ({
            userTag: jidToDisplayName(entry.userId),
            text: sanitizeDebateText(entry.text)
        }))
        .filter((entry) => entry.text.length >= 3)
        .map((entry) => `${entry.userTag}: ${entry.text}`);

    if (lines.length < 2) {
        await sock.sendMessage(remoteJid, { text: '🦫 No hay un debate claro aquí' }, { quoted: msg });
        return;
    }

    if (!GEMINI_API_KEY) {
        await sock.sendMessage(remoteJid, { text: getFallbackDebateText() }, { quoted: msg });
        return;
    }

    try {
        const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
        const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
        const prompt = getDebatePrompt(lines.join('\n'));
        const response = await model.generateContent(prompt);
        const output = normalizeDebateAiOutput(response?.response?.text() || '');

        if (!output || !output.includes('Tema:')) {
            await sock.sendMessage(remoteJid, { text: getFallbackDebateText() }, { quoted: msg });
            return;
        }

        await sock.sendMessage(remoteJid, { text: output }, { quoted: msg });
    } catch (error) {
        await sock.sendMessage(remoteJid, { text: getFallbackDebateText() }, { quoted: msg });
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

function scheduleReconnect(reason) {
    if (reconnectTimer) {
        return;
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
        if (isTimeoutLikeError(reason)) {
            scheduleReconnect('unhandled_timeout');
        }
    });
    process.on('uncaughtException', (error) => {
        console.error('UncaughtException detectado:', error?.message || error);
        if (isTimeoutLikeError(error)) {
            scheduleReconnect('uncaught_timeout');
            return;
        }
        scheduleReconnect('uncaught_error');
    });
}

async function startBot() {
    setupProcessErrorGuard();
    try {
        await ensureMongo();
    } catch (error) {
        console.error('No se pudo conectar MongoDB al iniciar:', error?.message || error);
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
        auth: state,
        browser: Browsers.ubuntu('Chrome'),
        connectTimeoutMs: BAILEYS_CONNECT_TIMEOUT_MS,
        defaultQueryTimeoutMs: BAILEYS_QUERY_TIMEOUT_MS,
        keepAliveIntervalMs: BAILEYS_KEEPALIVE_MS
    });

    const originalSendMessage = sock.sendMessage.bind(sock);
    sock.sendMessage = (jid, content, options) => {
        if (content?.react || content?.delete) {
            return originalSendMessage(jid, content, options);
        }
        const normalizedContent = { ...(content || {}) };
        const isDebateResult = typeof normalizedContent.text === 'string'
            && normalizedContent.text.includes('🦫 El castor analizó el debate');
        if (typeof normalizedContent.text === 'string' && !isDebateResult) {
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
                await new Promise((resolve) => setTimeout(resolve, totalDelay));
            }
            const result = await originalSendMessage(jid, normalizedContent, options);
            lastSentAtByJid.set(jid, Date.now());
            return result;
        };
        const queuedResult = globalSendQueue.catch(() => null).then(task);
        globalSendQueue = queuedResult.catch(() => null);
        return queuedResult;
    };

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            qrCodeData = qr;
        }

        if (connection === 'open') {
            qrCodeData = null;
            reconnectDelayMs = BOT_RECONNECT_BASE_MS;
            console.log('✅ BOT CONECTADO A WHATSAPP');
        }

        if (connection === 'close') {
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
        if (event.action !== 'add') {
            return;
        }

        for (const participantJid of event.participants) {
            try {
                if (isMongoReady) {
                    const record = await getModRecord(participantJid);
                    if (record?.isBanned) {
                        await upsertModRecord(participantJid, { $inc: { intentosReingreso: 1 } });
                        try {
                            await sock.groupParticipantsUpdate(event.id, [participantJid], 'remove');
                        } catch (error) {
                            await sendPrivateAdminMessage(sock, `🚨 ALERTA: El usuario baneado ${participantJid} intentó entrar al grupo ${event.id}, pero no pude expulsarlo automáticamente. Usa .unban ${participantJid} si deseas perdonarlo.`);
                            continue;
                        }
                        await sendPrivateAdminMessage(sock, `🚨 ALERTA: El usuario baneado ${participantJid} intentó entrar al grupo ${event.id}. Fue expulsado automáticamente. Usa .unban ${participantJid} si deseas perdonarlo.`);
                        continue;
                    }
                }
                await sendWelcome(sock, event.id, participantJid);
            } catch (error) {
                console.error('Error procesando ingreso al grupo:', error?.message || error);
            }
        }
    });

    sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            try {
                if (!msg.message || msg.key.fromMe) {
                    continue;
                }
                const remoteJid = msg.key.remoteJid;
                if (!remoteJid) {
                    continue;
                }
                const senderJid = msg.key.participant || msg.key.remoteJid;

                if (msg.message?.reactionMessage) {
                    await handleReactionReward(sock, msg, remoteJid);
                    continue;
                }

                const text = extractTextFromMessage(msg.message);
                let senderIsAdmin = false;
                if (remoteJid.endsWith('@g.us')) {
                    senderIsAdmin = await senderIsAuthorizedAdmin(sock, msg, remoteJid);
                }

                if (!senderIsAdmin) {
                    touchLastActivityAsync(senderJid);
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
                    continue;
                }

                if (!text || !text.trim().startsWith('.')) {
                    continue;
                }

                const command = text.trim().split(/\s+/)[0].toLowerCase();
                if (CASTOR_VALID_COMMANDS.has(command)) {
                    await sock.sendMessage(remoteJid, { react: { text: CASTOR_EMOJI, key: msg.key } });
                }
                if (command === '.reporte' || command === '.reportar') {
                    await handleReportCommand(sock, msg, text, remoteJid);
                } else if (command === '.advertir') {
                    await handleWarnCommand(sock, msg, text, remoteJid);
                } else if (command === '.unban') {
                    await handleUnbanCommand(sock, msg, text, remoteJid);
                } else if (command === '.sticker') {
                    await handleStickerCommand(sock, msg, remoteJid);
                } else if (command === '.fantasmas') {
                    await handleGhostsCommand(sock, msg, text, remoteJid);
                } else if (command === '.cerrar') {
                    await handleCloseGroupCommand(sock, msg, text, remoteJid);
                } else if (command === '.abrir') {
                    await handleOpenGroupCommand(sock, msg, remoteJid);
                } else if (command === '.pais') {
                    await handleSetCountryCommand(sock, msg, text, remoteJid);
                } else if (command === '.troncos') {
                    await handleTroncosCommand(sock, msg, remoteJid);
                } else if (command === '.ranking') {
                    await handleRankingCommand(sock, msg, remoteJid);
                } else if (command === '.dique') {
                    await handleDiqueCommand(sock, msg, remoteJid);
                } else if (command === '.perfil') {
                    await handlePerfilCommand(sock, msg, text, remoteJid);
                } else if (command === '.destacar') {
                    await handleDestacarCommand(sock, msg, text, remoteJid);
                } else if (command === '.evento') {
                    await handleEventoCommand(sock, msg, text, remoteJid);
                } else if (command === '.debatir') {
                    await handleDebatirCommand(sock, msg, remoteJid);
                }
            } catch (error) {
                console.error('Error en procesamiento de comando:', error?.message || error);
            }
        }
    });
}

startBot().catch((error) => {
    console.error('Error al iniciar bot:', error);
});
startKeepAlive();
