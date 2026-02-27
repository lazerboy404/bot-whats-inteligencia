const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    makeCacheableSignalKeyStore,
    fetchLatestBaileysVersion,
    delay,
    Browsers,
    proto,
    WAMessageStubType,
    initAuthCreds,
    BufferJSON
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const qrcode = require('qrcode-terminal');
const fs = require('fs');
const mongoose = require('mongoose');
const axios = require('axios'); // Cliente HTTP para buscar datasheets

// --- CONSTANTES GLOBALES DE CONFIGURACIÓN ---

// Lista de comandos especiales que requieren activación manual (Opt-in)
// Para agregar uno nuevo:
// 1. Añade el comando a este array: ['.ficha', '.coor', '.nuevo']
// 2. En la lógica del comando, añade: if (!config.allowedCommands.includes('.nuevo')) return;
const OPT_IN_COMMANDS = ['.ficha', '.coor']; 

// --- CONFIGURACIÓN SERPER API (Datasheets) ---
const SERPER_API_KEY = process.env.SERPER_API_KEY || ''; // Se espera que esté en Render

// --- POLYFILL PARA CRYPTO (Node 18/20 Fix) ---
if (!global.crypto) {
    global.crypto = require('crypto');
}

// --- CONFIGURACIÓN MONGODB (Persistencia de Sesión) ---
// Usa la variable de entorno o la cadena proporcionada si no existe
const MONGO_URL = process.env.MONGO_URL || "mongodb+srv://ntcrckrs_db_user:47V0ClnDyZIELB5E@cluster0.a6trfko.mongodb.net/?appName=Cluster0";
let AuthModel;

// Inicializamos el modelo SOLO UNA VEZ fuera de la función de conexión para evitar OverwriteModelError
const initMongoModel = () => {
    if (!AuthModel && mongoose.models.Auth) {
        AuthModel = mongoose.model('Auth');
    } else if (!AuthModel) {
        const AuthSchema = new mongoose.Schema({
            _id: String,
            data: String // Guardamos como JSON stringify
        });
        AuthModel = mongoose.model('Auth', AuthSchema);
    }
};

// --- MODELO DE CONFIGURACIÓN DE GRUPOS (Permisos) ---
let GroupConfigModel;
const initGroupConfigModel = () => {
    if (!GroupConfigModel && mongoose.models.GroupConfig) {
        GroupConfigModel = mongoose.model('GroupConfig');
    } else if (!GroupConfigModel) {
        const GroupConfigSchema = new mongoose.Schema({
            remoteJid: { type: String, required: true, unique: true },
            isWhitelistEnabled: { type: Boolean, default: false }, // false = todos permitidos (blacklist mode implicito vacío), true = solo allowedCommands
            allowedCommands: { type: [String], default: [] }
        });
        GroupConfigModel = mongoose.model('GroupConfig', GroupConfigSchema);
    }
};

// Caché en memoria para evitar consultas constantes a Mongo
const groupConfigCache = new Map();

const getGroupConfig = async (remoteJid) => {
    if (!GroupConfigModel) initGroupConfigModel();
    
    // 1. Buscar en caché
    if (groupConfigCache.has(remoteJid)) {
        return groupConfigCache.get(remoteJid);
    }

    // 2. Buscar en Mongo
    try {
        let config = await GroupConfigModel.findOne({ remoteJid });
        if (!config) {
            // Retornar default sin guardar para no llenar la DB de basura
            config = { isWhitelistEnabled: false, allowedCommands: [] };
        }
        // Guardar en caché (incluso si es default)
        groupConfigCache.set(remoteJid, config);
        return config;
    } catch (err) {
        console.error('Error obteniendo config de grupo:', err);
        return { isWhitelistEnabled: false, allowedCommands: [] }; // Fail-open (permitir todo si falla DB)
    }
};

const updateGroupConfig = async (remoteJid, update) => {
    if (!GroupConfigModel) initGroupConfigModel();
    try {
        const config = await GroupConfigModel.findOneAndUpdate(
            { remoteJid },
            update,
            { upsert: true, new: true, setDefaultsOnInsert: true }
        );
        // Actualizar caché
        groupConfigCache.set(remoteJid, config);
        return config;
    } catch (err) {
        console.error('Error actualizando config de grupo:', err);
        throw err;
    }
};

// Función para conectar (con reintento básico y log de éxito)
const connectToMongo = async () => {
    try {
        await mongoose.connect(MONGO_URL, {
            serverSelectionTimeoutMs: 5000 // Timeout de 5s para fallar rápido si la URL está mal
        });
        console.log('✅ Conectado a MongoDB con éxito');
        initMongoModel();
        initGroupConfigModel(); // Inicializar modelo de grupos
    } catch (err) {
        console.error('❌ Error CRÍTICO conectando a MongoDB:', err);
        throw err; // Re-lanzamos para detener el bot si la DB es obligatoria
    }
};

// Función personalizada para Auth con MongoDB
const useMongoAuthState = async () => {
    // Aseguramos que AuthModel exista
    if (!AuthModel) initMongoModel();

    const writeData = async (data, id) => {
        try {
            await AuthModel.updateOne(
                { _id: id },
                { $set: { data: JSON.stringify(data, BufferJSON.replacer) } },
                { upsert: true }
            );
        } catch (err) {
            console.error('Error escribiendo en Mongo:', err);
        }
    };

    const readData = async (id) => {
        try {
            const result = await AuthModel.findOne({ _id: id });
            if (result && result.data) {
                return JSON.parse(result.data, BufferJSON.reviver);
            }
            return null;
        } catch (err) {
            console.error('Error leyendo de Mongo:', err);
            return null;
        }
    };

    const removeData = async (id) => {
        try {
            await AuthModel.deleteOne({ _id: id });
        } catch (err) {
            console.error('Error borrando de Mongo:', err);
        }
    };

    const creds = await readData('creds') || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async id => {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value);
                        }
                        if (value) data[id] = value;
                    }));
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
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
        saveCreds: () => writeData(creds, 'creds')
    };
};

// --- CONFIGURACIÓN SERVIDOR (Render Keep-Alive) ---
const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;
let cachedCoordenadas = []; 

// --- COLAS DE PROCESAMIENTO (QUEUES) ---

// Set para deduplicación de mensajes (Evita procesar el mismo ID dos veces)
const processedMessageIds = new Set();
// Limpieza periódica de IDs (cada 1 hora)
setInterval(() => {
    console.log(`[SISTEMA] Limpiando caché de IDs procesados (${processedMessageIds.size} eliminados)`);
    processedMessageIds.clear();
}, 60 * 60 * 1000);

// Cola 0: ENTRADA (Raw Messages) - Desacopla recepción de procesamiento
const incomingQueue = [];
let isProcessingIncoming = false;
// Variable para el temporizador de Debounce/Buffer
let incomingBufferTimeout = null;

// --- CONFIGURACIÓN DE ADMIN Y CONTROL DE CHAT ---
// Agrega aquí los números permitidos (formato internacional sin +). Ejemplo: '5215512345678'
const ADMIN_NUMBERS = ['525564132674', '5215564132674', '146935243604020']; 
let isChatClosed = false;

// Cola 1: ACK (Aceptación Inmediata pero Segura)
// Objetivo: Responder "SOLICITUD ACEPTADA" rápido pero sin saturar (rate-limit)
const ackQueue = [];
let isProcessingAck = false;

// Cola 2: PROCESSING (Edición Lenta)
// Objetivo: Esperar 5s y editar el mensaje con las coordenadas
const processingQueue = [];
let isProcessingQueue = false;

// --- PROCESADORES ---

// 1. Procesador de ENTRADA (Filtrado y Regex)
async function processIncomingQueue(sock) {
    if (isProcessingIncoming || incomingQueue.length === 0) return;
    isProcessingIncoming = true;

    try {
        while (incomingQueue.length > 0) {
            const msg = incomingQueue.shift();
            
            try {
                // Filtros básicos
                if (!msg.message || msg.key.fromMe) continue;
                if (msg.message.protocolMessage || msg.message.reactionMessage) continue;

                // Extraer texto
                const msgType = Object.keys(msg.message)[0];
                const text = msgType === 'conversation' 
                    ? msg.message.conversation 
                    : msgType === 'extendedTextMessage' 
                        ? msg.message.extendedTextMessage.text 
                        : '';

                if (!text) continue;
                const remoteJid = msg.key.remoteJid;

                // --- CONTROL DE PERMISOS POR GRUPO (Whitelist) ---
                const cmdFull = text.trim();
                const cmdBase = cmdFull.split(' ')[0].toLowerCase(); // Ej: .ficha, .config

                // Comandos administrativos que SIEMPRE funcionan (bypass de permisos)
                // Solo validan permisos de administrador del grupo/bot internamente
                const ADMIN_COMMANDS = ['.config', '.add', '.remove', '.permit', '.off', '.on', '.silent', '.nosilent'];

                // Si NO es un comando administrativo y estamos en un grupo, verificar permisos
                // (Bloque de Modo Estricto eliminado por solicitud del usuario - Ahora es Opt-In por comando)
                /* 
                if (remoteJid.endsWith('@g.us') && !ADMIN_COMMANDS.includes(cmdBase) && cmdBase.startsWith('.')) {
                    const config = await getGroupConfig(remoteJid);
                    
                    if (config.isWhitelistEnabled) {
                        if (!config.allowedCommands.includes(cmdBase)) {
                            continue;
                        }
                    }
                } 
                */

                // --- FIN CONTROL PERMISOS ---

                // 1. CONTROL DEL BOT (Solo Dueño/SuperAdmins del Bot)
                if (cmdFull === '.off' || cmdFull === '.on' || cmdFull === '.cerrarbot' || cmdFull === '.abrirbot') {
                    // Identificar quién envía
                    const sender = msg.key.participant || msg.key.remoteJid;
                    const senderNumber = sender.replace(/\D/g, ''); // Solo números
                    const adminName = msg.pushName || 'El Administrador';

                    // Verificar si es admin del BOT (lista hardcodeada)
                    if (ADMIN_NUMBERS.includes(senderNumber)) {
                        if (cmdBase === '.off' || cmdBase === '.cerrarbot') {
                            if (!isChatClosed) {
                                isChatClosed = true;
                                await sock.sendMessage(remoteJid, { text: `� ${adminName} ha desactivado el bot.` }, { quoted: msg });
                            } else {
                                await sock.sendMessage(remoteJid, { text: '⚠️ El bot ya está desactivado.' }, { quoted: msg });
                            }
                        } else { // .on o .abrirbot
                            if (isChatClosed) {
                                isChatClosed = false;
                                await sock.sendMessage(remoteJid, { text: `� ${adminName} ha activado el bot.` }, { quoted: msg });
                            } else {
                                await sock.sendMessage(remoteJid, { text: '⚠️ El bot ya está activado.' }, { quoted: msg });
                            }
                        }
                    } else {
                        console.log(`[AUTH] Intento de comando admin denegado a: ${senderNumber}`);
                    }
                    continue; // Detener procesamiento
                }

                // 2. CONTROL DEL GRUPO (Silenciar/Activar - Para Admins del Grupo)
                if (cmdBase === '.silent' || cmdBase === '.nosilent') {
                    // a) Validación de Entorno (Solo Grupos)
                    if (!remoteJid.endsWith('@g.us')) {
                        await sock.sendMessage(remoteJid, { text: '⚠️ Este comando solo funciona en grupos.' }, { quoted: msg });
                        continue;
                    }

                    try {
                        // Obtener metadatos del grupo
                        const groupMetadata = await sock.groupMetadata(remoteJid);
                        const participants = groupMetadata.participants;

                        // b) Validación de Usuario (Solo Dueño del Bot)
                    const sender = msg.key.participant || msg.key.remoteJid;
                    const senderNumber = sender.replace(/\D/g, '');
                    const isBotOwner = ADMIN_NUMBERS.includes(senderNumber);

                    if (!isBotOwner) {
                         await sock.sendMessage(remoteJid, { text: '⛔ Acceso denegado: Solo el dueño del bot puede usar este comando.' }, { quoted: msg });
                         continue;
                    }

                        // c) Validación del Bot (Debe ser Admin del Grupo)
                        // Normalización segura de IDs para comparación (eliminar sufijos y @)
                        const botJid = sock.user?.id;
                        const botNumber = botJid ? botJid.split(':')[0].split('@')[0].replace(/\D/g, '') : '';
                        
                        // Búsqueda robusta del bot en la lista de participantes
                        let botParticipant = participants.find(p => {
                            const pNumber = p.id.split(':')[0].split('@')[0].replace(/\D/g, '');
                            return pNumber === botNumber;
                        });

                        // Fallback: búsqueda por inclusión si la exacta falla
                        if (!botParticipant) {
                            botParticipant = participants.find(p => p.id.includes(botNumber));
                        }
                        
                        const isBotAdmin = botParticipant?.admin === 'admin' || botParticipant?.admin === 'superadmin';

                        if (!isBotAdmin) {
                            // Depuración temporal para ver qué está pasando si falla
                            console.log(`[DEBUG SILENT] Bot Number: ${botNumber}`);
                            console.log(`[DEBUG SILENT] Found Participant:`, botParticipant);
                            
                            const debugMsg = `\n\n🔧 *Diagnóstico:*\nID Bot: ${botNumber || 'No detectado'}\nEncontrado: ${botParticipant ? 'SÍ' : 'NO'}\nRol: ${botParticipant?.admin || 'Miembro/Null'}`;

                            await sock.sendMessage(remoteJid, { text: '⚠️ Necesito ser administrador del grupo para ejecutar esta acción.' + debugMsg }, { quoted: msg });
                            continue;
                        }

                        // d) Ejecutar Acción
                        if (cmdBase === '.silent') {
                            await sock.groupSettingUpdate(remoteJid, 'announcement');
                            await sock.sendMessage(remoteJid, { text: '🔒 El grupo ha sido silenciado. Solo los administradores pueden enviar mensajes.' });
                        } else { // .nosilent
                            await sock.groupSettingUpdate(remoteJid, 'not_announcement');
                            await sock.sendMessage(remoteJid, { text: '🔓 El grupo ha sido abierto. Todos los participantes pueden escribir.' });
                        }

                    } catch (err) {
                        console.error('[GROUP ADMIN ERROR]', err);
                        // No enviamos error al chat para no spammear si falla algo interno
                    }
                    continue; // Detener procesamiento
                }

                // 2.5 GESTIÓN DE PERMISOS (.config, .add, .remove, .permit)
                if (cmdBase === '.config' || cmdBase === '.add' || cmdBase === '.remove' || cmdBase === '.permit') {
                    if (!remoteJid.endsWith('@g.us')) {
                        await sock.sendMessage(remoteJid, { text: '⚠️ Este comando solo funciona en grupos.' }, { quoted: msg });
                        continue;
                    }

                    // Verificar admin
                    const sender = msg.key.participant || msg.key.remoteJid;
                    const senderNumber = sender.replace(/\D/g, '');
                    const isBotOwner = ADMIN_NUMBERS.includes(senderNumber);

                    // .permit sigue siendo público para ver qué comandos funcionan
                    if (cmdBase !== '.permit' && !isBotOwner) {
                        await sock.sendMessage(remoteJid, { text: '⛔ Acceso denegado: Solo el dueño del bot puede configurar permisos.' }, { quoted: msg });
                        continue;
                    }

                    const args = cmdFull.split(' ').slice(1);
                    // Normalización de subcomandos para soportar .add, .remove y .permit directos
                    let subCmd, param;

                    if (cmdBase === '.add') {
                        subCmd = 'add';
                        param = args[0]?.toLowerCase(); // En .add el primer arg ya es el comando
                    } else if (cmdBase === '.remove') {
                        subCmd = 'remove';
                        param = args[0]?.toLowerCase();
                    } else if (cmdBase === '.permit') {
                        subCmd = 'list'; // .permit es alias de list
                    } else {
                        // Caso .config [subcmd] [param]
                        subCmd = args[0]?.toLowerCase();
                        param = args[1]?.toLowerCase();
                    }

                    if (subCmd === 'add' || subCmd === 'agregar') {
                        if (!param || !param.startsWith('.')) {
                            await sock.sendMessage(remoteJid, { text: '⚠️ Debes especificar el comando empezando con punto. Ejemplo: `.add .ficha`' });
                        } else {
                            const currentConfig = await getGroupConfig(remoteJid);
                            if (!currentConfig.allowedCommands.includes(param)) {
                                await updateGroupConfig(remoteJid, { $push: { allowedCommands: param } });
                                
                                let extraMsg = '';
                                if (param === '.coor') {
                                    extraMsg = '\n\n👁️ *Ojo:* Ahora el bot también buscará coordenadas automáticamente en este grupo cuando envíen IDs.';
                                }
                                
                                await sock.sendMessage(remoteJid, { text: `✅ Comando *${param}* agregado a la lista permitida.${extraMsg}` });
                            } else {
                                await sock.sendMessage(remoteJid, { text: `⚠️ El comando *${param}* ya estaba permitido.` });
                            }
                        }
                    } else if (subCmd === 'remove' || subCmd === 'quitar') {
                        if (!param) {
                            await sock.sendMessage(remoteJid, { text: '⚠️ Ejemplo: `.remove .ficha`' });
                        } else {
                            await updateGroupConfig(remoteJid, { $pull: { allowedCommands: param } });
                            
                            let extraMsg = '';
                            if (param === '.coor') {
                                extraMsg = '\n\n🙈 *Ojo:* La búsqueda automática de coordenadas se ha desactivado en este grupo.';
                            }

                            await sock.sendMessage(remoteJid, { text: `🗑️ Comando *${param}* eliminado de la lista permitida.${extraMsg}` });
                        }
                    } else if (subCmd === 'list' || subCmd === 'lista') {
                        // Comprobación dinámica de estado para comandos Opt-In
                        const config = await getGroupConfig(remoteJid);
                        
                        let statusList = '';
                        for (const cmd of OPT_IN_COMMANDS) {
                            // SOLO mostrar si está activo en el grupo
                            if (config.allowedCommands.includes(cmd)) {
                                statusList += `• *${cmd}*\n`;
                            }
                        }
                        
                        if (statusList === '') statusList = '(Ninguno)\n';

                        const message = `⚙️ *Configuración del Grupo*\n\n` +
                                        `✅ *Comandos Permitidos:*\n` +
                                        statusList;
                        
                        await sock.sendMessage(remoteJid, { text: message });
                    } else {
                        await sock.sendMessage(remoteJid, { 
                            text: `⚙️ *Ayuda de Configuración*\n\n` +
                                  `• *.add .comando*: Permite un comando\n` +
                                  `• *.remove .comando*: Bloquea un comando\n` +
                                  `• *.permit*: Ver comandos permitidos\n` +
                                  `• *.silent*: Cerrar grupo (Solo Admins)\n` +
                                  `• *.nosilent*: Abrir grupo (Todos)\n` +
                                  `• *.on*: Encender respuesta del Bot\n` +
                                  `• *.off*: Apagar respuesta del Bot`
                        });
                    }
                    continue;
                }

                // 3. COMANDO DATASHEET / FICHA TÉCNICA (Serper.dev)
                // Ejemplo: .ficha Axis P3245-V
                if (text.toLowerCase().startsWith('.ficha') || text.toLowerCase().startsWith('.datasheet')) {
                    
                    // Verificación Opt-In: .ficha requiere estar permitido explícitamente (igual que .coor)
                    const config = await getGroupConfig(remoteJid);
                    if (!config.allowedCommands.includes('.ficha')) {
                         // Si no está permitido, ignoramos silenciosamente
                         continue;
                    }

                    const model = text.split(' ').slice(1).join(' ');
                    
                    if (!model) {
                        await sock.sendMessage(remoteJid, { text: '⚠️ Debes escribir el modelo. Ejemplo: `.ficha DS-2CD2043G0-I`' }, { quoted: msg });
                        continue;
                    }

                    if (!process.env.SERPER_API_KEY) {
                        console.error('[SERPER ERROR] No API KEY configurada');
                        await sock.sendMessage(remoteJid, { text: '❌ Error de configuración: Falta la API Key de Serper.' }, { quoted: msg });
                        continue;
                    }

                    await sock.sendMessage(remoteJid, { text: `🔍 Buscando ficha técnica para: *${model}*...` }, { quoted: msg });

                    try {
                        // Construir consulta: Modelo + filtros para PDF
                        const query = `${model} datasheet OR ficha tecnica filetype:pdf`;
                        
                        const response = await axios.post('https://google.serper.dev/search', {
                            q: query,
                            num: 50 // Pedir más resultados (Cisco/HP tienen mucha "basura" documental)
                            // Eliminamos gl: 'mx' y hl: 'es' para permitir resultados globales (inglés/oficiales)
                        }, {
                            headers: {
                                'X-API-KEY': process.env.SERPER_API_KEY,
                                'Content-Type': 'application/json'
                            }
                        });

                        const organicResults = response.data.organic;

                        if (organicResults && organicResults.length > 0) {
                            // Lista de dominios oficiales de fabricantes CCTV/Redes/TI
                            const OFFICIAL_DOMAINS = [
                                // CCTV & Acceso
                                'hikvision.com', 'hikvision.com.mx', 'hik-connect.com', 'ezviz.com', 'ezvizlife.com',
                                'dahuasecurity.com', 'dahua-security.com', 'imoulife.com',
                                'axis.com', 'avigilon.com', 'boschsecurity.com', 'bosch.com',
                                'honeywell.com', 'samsung.com', 'hanwhavision.com', 'hanwha-security.com',
                                'vivotek.com', 'uniview.com', 'uniview.com.cn', 'unv.com',
                                'zkteco.com', 'zktecolatinoamerica.com', 'supremainc.com', 'suprema.co.kr',
                                'assaabloy.com', 'rosslare.com', 'cdvi.com', 'hidglobal.com', 'hid.com',
                                'paradox.com', 'dsc.com', 'resideo.com', 'ajax.systems', 'riscogroup.com', 'garrett.com',
                                'zkteco.mx', 'zkteco.us',
                                
                                // VMS & Software de Seguridad
                                'genetec.com', 'milestonesys.com', 'issivs.com', 'isscctv.com', 'axxonsoft.com', 'digifort.com',

                                // Redes & Conectividad
                                'ubnt.com', 'ui.com', 'cisco.com', 'meraki.com', 'arubanetworks.com', 'ruijienetworks.com', 'ruijie.com.cn',
                                'mikrotik.com', 'cambiumnetworks.com', 'tplink.com', 'tp-link.com', 'netgear.com', 
                                'grandstream.com', 'fanvil.com', 'juniper.net', 'fortinet.com', 'paloaltonetworks.com',
                                'sonicwall.com', 'sophos.com', 'watchguard.com', 'huawei.com', 'zte.com.cn',
                                
                                // Infraestructura & Cableado
                                'panduit.com', 'belden.com', 'commscope.com', 'siemon.com', 'legrand.com',
                                'toten.com.cn', 'linkedpro.com', 'linkedpro.mx', // LinkedPro es marca propia de Syscom
                                'charofil.com', 'charofil.mx',
                                
                                // Energía
                                'apc.com', 'schneider-electric.com', 'tripplite.com', 'cyberpower.com', 'epcom.net', 'epcom.com.mx',
                                
                                // Almacenamiento & Computación (Servidores/Workstations)
                                'kingston.com', 'adata.com', 'westerndigital.com', 'wd.com', 'seagate.com', 
                                'toshiba.com', 'sandisk.com', 'crucial.com', 'dell.com', 'delltechnologies.com', 
                                'lenovo.com', 'hp.com', 'hpe.com', 'intel.com', 'amd.com',
                                
                                // Audio/Video & Proyección
                                'barco.com', 'epson.com', 'benq.com', 'lg.com', 'samsung.com', 'sony.com', 'christiedigital.com',
                                'jbl.com', 'bose.com', 'shure.com', 'senheiser.com'
                            ];

                            let bestResult = null;
                            let sourceType = 'GENERIC'; // OFFICIAL, SYSCOM, GENERIC

                            // 1. Prioridad: PDF en dominio oficial
                            // Primero filtramos todos los candidatos oficiales
                            const officialCandidates = organicResults.filter(r => {
                                const link = r.link.toLowerCase();
                                const title = r.title.toLowerCase();
                                const isPdf = link.endsWith('.pdf') || title.includes('datasheet') || title.includes('ficha');
                                const isOfficialDomain = OFFICIAL_DOMAINS.some(d => link.includes(d));
                                return isPdf && isOfficialDomain;
                            });

                            if (officialCandidates.length > 0) {
                                sourceType = 'OFFICIAL';
                                
                                // Definir qué constituye una "Ficha Técnica Real" vs "Manual/Guía"
                                // Esto ayuda a evitar "Installation Guide", "Quick Start Guide", "User Manual" si existe un Datasheet real.
                                const isRealDatasheet = (r) => {
                                    const text = (r.title + r.link).toLowerCase();
                                    
                                    // FILTRO NEGATIVO: Si dice "Guía", "Manual", "Install", etc., NO es un datasheet puro.
                                    // Esto penaliza documentos como "Short Guide", "Quick Start", "Hardware Installation", "Differences".
                                    const badWords = [
                                        'guide', 'guia', 'manual', 'install', 'setup', 'short', 'qsg', 'hig', 'quick', 'breve', 'usuario', 'user', 'start', 'inicio', 'montaje',
                                        'differences', 'diferencias', 'comparison', 'comparativa', 'versus', 'vs',
                                        'flyer', 'folleto', 'brochure', 'catalog', 'catalogo', 'list', 'lista', 'eol', 'eos', 'announcement', 'notice'
                                    ];
                                    if (badWords.some(w => text.includes(w))) return false;

                                    // FILTRO POSITIVO: Debe decir explícitamente Datasheet o Ficha Técnica
                                    return text.includes('datasheet') || 
                                           text.includes('data-sheet') || 
                                           text.includes('data_sheet') || 
                                           text.includes('spec sheet') || 
                                           text.includes('especificaciones') ||
                                           (text.includes('ficha') && text.includes('tecnica'));
                                };

                            // Separar candidatos en "Datasheets Puros" y "Otros (Manuales, etc)"
                                const datasheets = officialCandidates.filter(isRealDatasheet);
                                const others = officialCandidates.filter(r => !isRealDatasheet(r));

                                // Buscar posible resultado en Syscom para comparar
                                const syscomResult = organicResults.find(r => {
                                    const link = r.link.toLowerCase();
                                    const title = r.title.toLowerCase();
                                    const isPdf = link.endsWith('.pdf') || title.includes('datasheet') || title.includes('ficha');
                                    return isPdf && link.includes('syscom');
                                });

                                // Función para preferir español
                                const preferSpanish = (candidates) => {
                                    return candidates.find(r => {
                                        const text = (r.title + r.snippet).toLowerCase();
                                        return text.includes('ficha') || text.includes('técnica') || text.includes('tecnica') || text.includes('manual de usuario') || text.includes('spanish') || text.includes('es-es') || text.includes('es_mx');
                                    });
                                };

                                let bestOfficial = null;

                                // 1. Buscar Datasheet en Español
                                const spanishDatasheet = preferSpanish(datasheets);
                                
                                if (spanishDatasheet) {
                                    bestOfficial = spanishDatasheet;
                                } else if (datasheets.length > 0) {
                                    // 2. Si no hay Datasheet Español, usar Datasheet Inglés (MEJOR QUE MANUAL ESPAÑOL)
                                    bestOfficial = datasheets[0];
                                } 
                                
                                // Si encontramos un Datasheet Oficial, nos quedamos con él.
                                if (bestOfficial) {
                                    bestResult = bestOfficial;
                                    sourceType = 'OFFICIAL';
                                } else {
                                    // NO hay Datasheet Oficial Puro (solo Manuales/Guías en 'others')
                                    
                                    // 3. Verificar si Syscom tiene algo antes de caer en "Manuales Oficiales"
                                    // Esto soluciona el caso Cisco: El sitio oficial llena de Guías, pero Syscom tiene la Ficha.
                                    if (syscomResult) {
                                        bestResult = syscomResult;
                                        sourceType = 'SYSCOM';
                                    } else {
                                        // 4. Si no hay Syscom, entonces sí usamos los Manuales/Guías Oficiales
                                        const spanishOther = preferSpanish(others);
                                        if (spanishOther) {
                                            bestOfficial = spanishOther;
                                        } else {
                                            bestOfficial = others[0];
                                        }
                                        bestResult = bestOfficial || officialCandidates[0];
                                        sourceType = 'OFFICIAL';
                                    }
                                }

                            } else {
                                // 2. Prioridad: PDF en Syscom (Distribuidor) - Si no hubo NINGÚN resultado oficial
                                const syscomResult = organicResults.find(r => {
                                    const link = r.link.toLowerCase();
                                    const title = r.title.toLowerCase();
                                    const isPdf = link.endsWith('.pdf') || title.includes('datasheet') || title.includes('ficha');
                                    return isPdf && link.includes('syscom');
                                });

                                if (syscomResult) {
                                    bestResult = syscomResult;
                                    sourceType = 'SYSCOM';
                                } else {
                                    // 3. Prioridad: Cualquier PDF (Externo)
                                    bestResult = organicResults.find(r => r.link.toLowerCase().endsWith('.pdf'));
                                }
                            }

                            // 4. Fallback: El primer resultado orgánico (aunque no sea PDF, si no hay nada más)
                            if (!bestResult) {
                                bestResult = organicResults[0];
                            }
                            
                            const title = bestResult.title || 'Ficha Técnica';
                            const link = bestResult.link;
                            const snippet = bestResult.snippet || 'Documento encontrado.';
                            
                            let sourceTag = '⚠️ *Fuente Externa*';
                            if (sourceType === 'OFFICIAL') sourceTag = '✅ *Fuente Oficial*';
                            if (sourceType === 'SYSCOM') sourceTag = '🛒 *Distribuidor (Syscom)*';

                            console.log(`[SERPER] Resultado seleccionado (${sourceType}): ${title} -> ${link}`);

                            const caption = `📄 *Ficha Técnica Encontrada*\n\n📌 *Modelo:* ${model}\n${sourceTag}\n📝 *Título:* ${title}\n� *Link:* ${link}\n\n> ${snippet}`;

                            // Intentar enviar el PDF directamente
                            try {
                                await sock.sendMessage(remoteJid, { 
                                    document: { url: link }, 
                                    mimetype: 'application/pdf', 
                                    fileName: `${model.replace(/\s+/g, '_')}_Datasheet.pdf`,
                                    caption: caption
                                }, { quoted: msg });
                            } catch (sendError) {
                                console.error('[BAILEYS FILE ERROR]', sendError.message);
                                // Fallback: Enviar solo texto con el link si falla la descarga/envío
                                await sock.sendMessage(remoteJid, { 
                                    text: `⚠️ No pude enviar el archivo directamente (posiblemente muy pesado o protegido), pero aquí tienes el enlace:\n\n${caption}` 
                                }, { quoted: msg });
                            }

                        } else {
                            await sock.sendMessage(remoteJid, { text: `❌ No encontré ninguna ficha técnica para "${model}". Intenta ser más específico.` }, { quoted: msg });
                        }

                    } catch (error) {
                        console.error('[SERPER API ERROR]', error?.response?.data || error.message);
                        await sock.sendMessage(remoteJid, { text: '❌ Error al buscar en internet. Intenta más tarde.' }, { quoted: msg });
                    }
                    continue;
                }

                // Si el chat está cerrado, ignorar mensajes (excepto los comandos de admin arriba)
                if (isChatClosed) {
                    continue;
                }

                // Anti-Bucle (Ignorar respuestas propias citadas o forwards del bot)
                if (text.includes('SOLICITUD ACEPTADA') || text.includes('Cuadrilla 𝗕𝗼𝘁')) continue;

                console.log(`[INCOMING] Procesando msg de ${remoteJid}: ${text.substring(0, 30)}...`);

                // Comandos Simples
                if (text === '.ping') {
                    const sender = msg.key.participant || msg.key.remoteJid;
                    const senderNumber = sender.replace(/\D/g, '');
                    if (ADMIN_NUMBERS.includes(senderNumber)) {
                        await sock.sendMessage(remoteJid, { text: 'pong!' }, { quoted: msg });
                    }
                    continue;
                }


                // 4. COORDENADAS (.coor) y Búsqueda Implícita
                // Busca IDs (MC12345 o 12345) en el mensaje.
                
                // 4. COORDENADAS (.coor) y Búsqueda Implícita
                // Busca IDs (MC12345 o 12345) en el mensaje.
                
                const config = await getGroupConfig(remoteJid);
                const isCoorEnabled = config.allowedCommands.includes('.coor');

                if (isCoorEnabled) {
                    // Regex mejorado: 
                    // 1. (?<!\.) Evita coincidir con decimales de coordenadas (ej: 19.12345)
                    // 2. \b Límite de palabra
                    // 3. (?:[A-Z]+[:\s]*)? Soporta CUALQUIER prefijo de letras mayúsculas (MC, MMC, MCMC, ID, etc)
                    // 4. (\d{5}) Captura exactamente 5 dígitos
                    const coordMatches = [...text.matchAll(/(?<!\.)\b(?:[A-Z]+[:\s]*)?(\d{5})\b/gi)];

                    // --- DETECCIÓN DE FORMATO INCORRECTO ---
                    // Solo si NO hay matches válidos, buscamos intentos fallidos (MC + num incorrecto)
                    // para avisar al usuario. NO avisamos por números sueltos (evita falsos positivos en chat normal)
                    if (coordMatches.length === 0) {
                        const invalidMatches = [...text.matchAll(/\b(?:M{1,2}C[:\s]*|ID[:\s]*)(\d{1,4}|\d{6,})\b/gi)];
                        if (invalidMatches.length > 0) {
                             const badId = invalidMatches[0][1];
                             await sock.sendMessage(remoteJid, { text: `⚠️ *Formato Incorrecto*\n\nDetecté un intento de ID (${badId}) pero no tiene 5 dígitos.\nPor favor verifica el número.` }, { quoted: msg });
                             continue;
                        }
                    }

                    if (coordMatches.length > 0) {
                        // Solo procesar si es explícitamente .coor O si es texto normal (búsqueda implícita)
                        if (cmdBase === '.coor' || !text.startsWith('.')) {
                            
                            const uniqueIDs = new Set();
                            const validMatches = [];

                            for (const match of coordMatches) {
                                const idNumbers = match[1];
                                if (!uniqueIDs.has(idNumbers)) {
                                    uniqueIDs.add(idNumbers);
                                    validMatches.push(idNumbers);
                                }
                            }

                            if (validMatches.length > 0) {
                                console.log(`[MATCH] ${validMatches.length} IDs únicos encontrados (Permiso: OK)`);
                                for (const idNumbers of validMatches) {
                                    const idToFind = `MC${idNumbers}`;
                                    ackQueue.push({ msg, remoteJid, idToFind });
                                }
                                runAckQueue(sock);
                            }
                            continue;
                        }
                    } else if (cmdBase === '.coor') {
                        // Habilitado, comando correcto, pero sin IDs válidos
                        await sock.sendMessage(remoteJid, { text: '⚠️ Formato incorrecto. Debes incluir el ID de 5 dígitos (ej: 12345).' }, { quoted: msg });
                        continue;
                    }
                }
                // Si no está habilitado, simplemente ignoramos (tanto comando como búsqueda implícita)

            } catch (err) {
                console.error('[INCOMING ERROR]', err);
            }
        }
    } finally {
        isProcessingIncoming = false;
        // Doble check por si entraron mensajes mientras procesábamos
        if (incomingQueue.length > 0) processIncomingQueue(sock);
    }
}

// 2. Procesador de ACK (Envío inicial)
async function runAckQueue(sock) {
    if (isProcessingAck || ackQueue.length === 0) return;
    isProcessingAck = true;

    try {
        while (ackQueue.length > 0) {
            const { msg, remoteJid, idToFind } = ackQueue.shift();
            
            try {
                // Simular Escribiendo
                await sock.sendPresenceUpdate('composing', remoteJid);
                
                // Generar texto
                const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
                const date = new Date();
                const formattedDate = `${date.getDate()} de ${months[date.getMonth()]}`;
                const acceptanceText = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;

                // ENVIAR
                let sentMsg;
                try {
                    sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });
                } catch (sendError) {
                    console.error(`[ACK-RETRY] Error enviando a ${idToFind}, reintentando...`);
                    await delay(1000);
                    sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });
                }

                if (sentMsg) {
                    processingQueue.push({ sentMsg, userMsg: msg, remoteJid, idToFind });
                    runProcessor(sock); // Disparar cola de edición
                }

                // Delay entre ACKs para no saturar
                await delay(800); 

            } catch (err) {
                console.error(`[ACK ERROR] ID ${idToFind}:`, err);
            }
        }
    } finally {
        isProcessingAck = false;
        if (ackQueue.length > 0) runAckQueue(sock);
    }
}

// 3. Procesador de EDICIÓN (Búsqueda y Resultado)
async function runProcessor(sock) {
    if (isProcessingQueue || processingQueue.length === 0) return;
    isProcessingQueue = true;

    try {
        while (processingQueue.length > 0) {
            const { sentMsg, userMsg, remoteJid, idToFind } = processingQueue.shift();

            console.log(`[PROCESSOR] Editando ID: ${idToFind}. Pendientes: ${processingQueue.length}`);

            try {
                // Delay 5s (Efecto búsqueda)
                await delay(5000);

                // Buscar en Caché
                const found = cachedCoordenadas.find(item => item.ID === idToFind);
                
                const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
                const date = new Date();
                const formattedDate = `${date.getDate()} de ${months[date.getMonth()]}`;

                if (found) {
                    const lat = found.Latitud || found.lat || 'No definida';
                    const long = found.Longitud || found.long || 'No definida';
                    
                    const finalText = `🆔: ${idToFind}\n${lat} ${long}`;

                    await sock.sendMessage(remoteJid, { text: finalText, edit: sentMsg.key });
                    await sock.sendMessage(remoteJid, { react: { text: '👨‍💻', key: userMsg.key } });
                    
                } else {
                    const notFoundText = `❌ No se encontraron coordenadas para el ID: ${idToFind}\n\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
                    await sock.sendMessage(remoteJid, { text: notFoundText, edit: sentMsg.key });
                }

                // Pausa breve
                await delay(500);

            } catch (err) {
                console.error(`[PROCESSOR ERROR] ${idToFind}:`, err);
            }
        }
    } finally {
        isProcessingQueue = false;
        if (processingQueue.length > 0) runProcessor(sock);
    }
}


// --- RUTAS WEB ---
app.get('/', (req, res) => {
    if (qrCodeData) {
        res.send(`
            <html>
                <head>
                    <title>Bot Baileys - QR</title>
                    <meta http-equiv="refresh" content="5">
                    <style>body{font-family:sans-serif;text-align:center;padding:50px;}</style>
                </head>
                <body>
                    <h1>🤖 Bot WhatsApp (Baileys Light)</h1>
                    <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(qrCodeData)}" />
                    <p>Escanea rápido. Se actualiza cada 5s.</p>
                </body>
            </html>
        `);
    } else {
        res.send(`
            <html>
                <head><title>Bot Activo</title></head>
                <body style="font-family:sans-serif;text-align:center;padding:50px;">
                    <h1>✅ Bot Conectado y Listo</h1>
                    <p>Memoria JSON: ${cachedCoordenadas.length} registros cargados.</p>
                    <p>Uso de RAM: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)} MB</p>
                    <p>Si no responde, revisa los logs en Render.</p>
                </body>
            </html>
        `);
    }
});

// SELF-PING PARA MANTENER ACTIVO (Render Free Tier)
// Render Free se duerme tras 15 min de inactividad. Esto ayuda a mantenerlo despierto un poco más.
setInterval(() => {
    const renderUrl = process.env.RENDER_EXTERNAL_URL;
    if (renderUrl) {
        console.log(`[KEEP-ALIVE] Ping a ${renderUrl}`);
        fetch(renderUrl).catch(() => {});
    }
}, 10 * 60 * 1000); // Cada 10 minutos

app.listen(PORT, () => console.log(`Servidor iniciado en puerto ${PORT}`));

// --- LÓGICA DE CACHÉ JSON (Igual que antes) ---
async function loadCoordenadas() {
    console.log('[SISTEMA] Cargando JSON de coordenadas...');
    try {
        const response = await fetch('https://raw.githubusercontent.com/lazerboy404/buscador-totems/main/coordenadas-script.json');
        if (!response.ok) throw new Error(response.statusText);
        const data = await response.json();
        cachedCoordenadas = data;
        console.log(`[SISTEMA] ✅ ${data.length} coordenadas en memoria.`);
    } catch (e) {
        console.error('[SISTEMA] Error cargando JSON:', e);
        setTimeout(loadCoordenadas, 60000);
    }
}
loadCoordenadas();

// --- LÓGICA DEL BOT (Baileys) ---
async function startBot() {
    // Auth State: Elegir entre Mongo o Archivos
    let state, saveCreds;

    if (MONGO_URL) {
        console.log('Intentando conectar a MongoDB...');
        
        try {
            // AWAIT CRÍTICO: Esperamos a que la conexión esté lista ANTES de seguir
            if (mongoose.connection.readyState === 0) {
                await connectToMongo();
            }
            
            const auth = await useMongoAuthState();
            state = auth.state;
            saveCreds = auth.saveCreds;
            
        } catch (mongoError) {
            console.error('FALLO FATAL EN MONGO, USANDO ARCHIVOS LOCALES (Volátil en Render):', mongoError);
            const auth = await useMultiFileAuthState('auth_info_baileys');
            state = auth.state;
            saveCreds = auth.saveCreds;
        }

    } else {
        console.log('Using FileSystem Auth Strategy...');
        const auth = await useMultiFileAuthState('auth_info_baileys');
        state = auth.state;
        saveCreds = auth.saveCreds;
    }

    const { version } = await fetchLatestBaileysVersion();
    console.log(`Usando Baileys v${version.join('.')}`);

    const sock = makeWASocket({
        version,
        logger: pino({ level: 'info' }), // Habilitamos logs INFO para depurar
        printQRInTerminal: true, // QR en consola
        auth: {
            creds: state.creds,
            // CLAVE: makeCacheableSignalKeyStore evita el error "No sessions" en grupos
            keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
        },
        generateHighQualityLinkPreview: true,
        // Usamos la configuración de navegador recomendada para Ubuntu/Linux (Render)
        browser: Browsers.ubuntu('Chrome'), 
        syncFullHistory: false, // Ahorra memoria al no sincronizar todo el historial antiguo
        markOnlineOnConnect: true, // Marcar online al conectar
        defaultQueryTimeoutMs: 60000, // Aumentar timeout a 60s
    });

    // Eventos de Conexión
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            qrCodeData = qr;
            console.log('NUEVO QR GENERADO');
        }

        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect.error)?.output?.statusCode !== DisconnectReason.loggedOut;
            
            // Loguear el error exacto
            const errorCode = (lastDisconnect.error)?.output?.statusCode;
            const errorReason = (lastDisconnect.error)?.output?.payload?.error || (lastDisconnect.error)?.message;
            console.error(`Conexión cerrada. Código: ${errorCode}, Razón: ${errorReason}`);
            console.log('¿Debería reconectar?:', shouldReconnect);

            if (shouldReconnect) {
                // Si es un error 515 (Stream Error), a veces es mejor esperar un poco
                if (errorCode === 515) {
                    console.log('Error de Stream (515). Esperando 5s antes de reintentar...');
                    setTimeout(startBot, 5000);
                } else {
                    startBot();
                }
            } else {
                console.log('Sesión cerrada permanentemente (Logged Out). Borra la carpeta auth_info_baileys y reinicia.');
                // Opcional: Borrar carpeta automáticamente
                fs.rmSync('auth_info_baileys', { recursive: true, force: true });
                startBot(); // Reiniciar para generar nuevo QR
            }
        } else if (connection === 'open') {
            console.log('✅ BOT CONECTADO A WHATSAPP');
            qrCodeData = null;
        }
    });

    // Guardar credenciales
    sock.ev.on('creds.update', saveCreds);

    // Manejo de Mensajes (UPSERT - Entrada Cruda)
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        // Log detallado de entrada
        console.log(`[UPSERT] Recibido evento. Tipo: ${type}. Cantidad: ${messages.length}`);

        for (const msg of messages) {
            // Deduplicación Crítica
            if (processedMessageIds.has(msg.key.id)) {
                console.log(`[DUPLICADO] ID ${msg.key.id} ya procesado. Ignorando.`);
                continue;
            }
            
            // Añadir a procesados
            processedMessageIds.add(msg.key.id);

            // Push a Cola de Entrada
            incomingQueue.push(msg);
        }

        // --- LÓGICA DE DEBOUNCE / BUFFER ---
        // Si hay un temporizador corriendo, lo limpiamos (reiniciamos la cuenta atrás)
        // Esto permite "agrupar" ráfagas. Si siguen llegando mensajes, seguimos esperando.
        if (incomingBufferTimeout) {
            clearTimeout(incomingBufferTimeout);
        }

        // Establecemos un nuevo temporizador. 
        // Solo procesaremos la cola si dejan de llegar mensajes por 1000ms (1 segundo).
        // Opcional: Podríamos poner un límite máximo de espera si quisiéramos.
        console.log(`[BUFFER] Esperando 1.5s para estabilizar ráfaga... (Cola: ${incomingQueue.length})`);
        
        incomingBufferTimeout = setTimeout(() => {
            console.log('[BUFFER] Tiempo de espera finalizado. Procesando lote acumulado.');
            processIncomingQueue(sock);
            incomingBufferTimeout = null;
        }, 1500); // 1.5 segundos de espera tras el último mensaje
    });
}

startBot().catch(err => console.error('Error al iniciar bot:', err));