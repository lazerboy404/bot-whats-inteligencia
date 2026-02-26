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

// Función para conectar (con reintento básico y log de éxito)
const connectToMongo = async () => {
    try {
        await mongoose.connect(MONGO_URL, {
            serverSelectionTimeoutMs: 5000 // Timeout de 5s para fallar rápido si la URL está mal
        });
        console.log('✅ Conectado a MongoDB con éxito');
        initMongoModel();
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
const ADMIN_NUMBERS = ['525564132674', '5215564132674']; 
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

                // --- CONTROL DE ADMIN (Cerrar/Abrir Chat) ---
                if (text.trim() === '.cerrar' || text.trim() === '.abrir') {
                    // Identificar quién envía (soporte para grupos y chat privado)
                    const sender = msg.key.participant || msg.key.remoteJid;
                    const senderNumber = sender.replace(/\D/g, ''); // Solo números
                    const adminName = msg.pushName || 'El Administrador';

                    // Verificar si es admin
                    if (ADMIN_NUMBERS.includes(senderNumber)) {
                        if (text.trim() === '.cerrar') {
                            if (!isChatClosed) {
                                isChatClosed = true;
                                await sock.sendMessage(remoteJid, { text: `🔒 ${adminName} ha cerrado el chat.` }, { quoted: msg });
                            } else {
                                await sock.sendMessage(remoteJid, { text: '⚠️ El chat ya está cerrado.' }, { quoted: msg });
                            }
                        } else { // .abrir
                            if (isChatClosed) {
                                isChatClosed = false;
                                await sock.sendMessage(remoteJid, { text: `🔓 ${adminName} ha abierto el chat.` }, { quoted: msg });
                            } else {
                                await sock.sendMessage(remoteJid, { text: '⚠️ El chat ya está abierto.' }, { quoted: msg });
                            }
                        }
                    } else {
                        console.log(`[AUTH] Intento de comando admin denegado a: ${senderNumber}`);
                    }
                    continue; // Detener procesamiento
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
                    await sock.sendMessage(remoteJid, { text: 'pong!' }, { quoted: msg });
                    continue;
                }
                if (text === '.menuprincipal') {
                    await sock.sendMessage(remoteJid, { text: 'Bot Activo ⚡' }, { quoted: msg });
                    continue;
                }

                // Lógica MC (Regex)
                const matches = [...text.matchAll(/MC[:\s]*(\d+)/gi)];
                if (matches.length > 0) {
                    console.log(`[MATCH] ${matches.length} IDs encontrados en mensaje.`);
                    
                    for (const match of matches) {
                        const fullMatch = match[0];
                        const idNumbers = match[1];
                        const idToFind = `MC${idNumbers}`;

                        // Validación
                        if (idNumbers.length !== 5) {
                            if (matches.length === 1) {
                                await sock.sendMessage(remoteJid, { text: 'ID INVÁLIDO (Debe ser de 5 dígitos)' }, { quoted: msg });
                            }
                            continue;
                        }

                        // Push a ACK Queue
                        ackQueue.push({ msg, remoteJid, idToFind });
                    }
                    // Disparar siguiente cola
                    runAckQueue(sock);
                }

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