const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers, downloadContentFromMessage, initAuthCreds, BufferJSON, proto } = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const mongoose = require('mongoose');

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
const CASTOR_EMOJI = '🦫';
const CASTOR_DEFAULT_IMAGE_URL = process.env.CASTOR_DEFAULT_IMAGE_URL || 'https://raw.githubusercontent.com/github/explore/main/topics/mongodb/mongodb.png';
const CASTOR_SEAL_STICKER_URL = process.env.CASTOR_SEAL_STICKER_URL || '';
const CASTOR_VALID_COMMANDS = new Set(['.reporte', '.advertir', '.unban', '.sticker', '.fantasmas', '.cerrar', '.abrir']);
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
    let text = sanitizeText(value ?? '', 9000);
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
        '📌 *Reglas básicas del grupo:*',
        '1) Respeto total entre miembros.',
        '2) Prohibidos insultos, amenazas o acoso.',
        '3) No spam, cadenas ni contenido ofensivo.',
        '4) Evita compartir datos personales de terceros.',
        '5) Sigue indicaciones de los administradores.'
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
            ultimaActividad: { type: Date, default: null }
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

async function sendWelcome(sock, groupJid, participantJid) {
    const number = getNumberFromJid(participantJid);
    const mention = `@${number}`;
    const country = getCountryFromNumber(number);
    const flag = getFlagFromNumber(number);
    const welcomeText = `¡Un nuevo castor ha llegado al estanque! ${CASTOR_EMOJI} Bienvenido/a ${mention}. Nos saludas desde ${country} ${flag}. Soy Castor Bot, el guardián de este dique. 🪵 ¡Ponte cómodo y ayudemos a construir!`;

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
            mentions: [participantJid]
        });
    } catch (error) {
        await sock.sendMessage(groupJid, {
            text: welcomeText,
            mentions: [participantJid]
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

async function startBot() {
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
        browser: Browsers.ubuntu('Chrome')
    });

    const originalSendMessage = sock.sendMessage.bind(sock);
    sock.sendMessage = (jid, content, options) => {
        if (content?.react || content?.delete) {
            return originalSendMessage(jid, content, options);
        }
        const normalizedContent = { ...(content || {}) };
        if (typeof normalizedContent.text === 'string') {
            normalizedContent.text = brandCastorText(normalizedContent.text);
        }
        if (typeof normalizedContent.caption === 'string') {
            normalizedContent.caption = brandCastorText(normalizedContent.caption);
        }
        globalSendQueue = globalSendQueue.then(async () => {
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
        });
        return globalSendQueue;
    };

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            qrCodeData = qr;
        }

        if (connection === 'open') {
            qrCodeData = null;
            console.log('✅ BOT CONECTADO A WHATSAPP');
        }

        if (connection === 'close') {
            const code = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = code !== DisconnectReason.loggedOut;
            if (shouldReconnect) {
                startBot();
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
                if (command === '.reporte') {
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
