const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    makeCacheableSignalKeyStore,
    fetchLatestBaileysVersion,
    delay,
    Browsers,
    proto,
    WAMessageStubType
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const qrcode = require('qrcode-terminal');
const fs = require('fs');

// --- POLYFILL PARA CRYPTO (Node 18/20 Fix) ---
if (!global.crypto) {
    global.crypto = require('crypto');
}

// --- CONFIGURACIÓN SERVIDOR (Render Keep-Alive) ---
const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;
let cachedCoordenadas = []; 

// --- COLAS DE PROCESAMIENTO (QUEUES) ---

// Cola 1: ACK (Aceptación Inmediata pero Segura)
// Objetivo: Responder "SOLICITUD ACEPTADA" rápido pero sin saturar (rate-limit)
const ackQueue = [];
let isProcessingAck = false;

// Cola 2: PROCESSING (Edición Lenta)
// Objetivo: Esperar 5s y editar el mensaje con las coordenadas
const processingQueue = [];
let isProcessingQueue = false;

// Procesador de ACK (Envío inicial)
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

                // ENVIAR (Con reintento básico si falla por rate-limit)
                let sentMsg;
                try {
                    sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });
                } catch (sendError) {
                    console.error(`[ACK-QUEUE] Error enviando a ${idToFind}, reintentando en 1s...`, sendError);
                    await delay(1000);
                    sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });
                }

                // Si se envió correctamente, pasar a la siguiente cola
                if (sentMsg) {
                    processingQueue.push({ sentMsg, userMsg: msg, remoteJid, idToFind });
                    runProcessor(sock); // Disparar la cola de edición
                }

                // Delay de seguridad entre ACKs (evita rate-overlimit en ráfagas)
                await delay(1000); 

            } catch (err) {
                console.error(`[ACK-QUEUE] Error fatal con ID ${idToFind}:`, err);
            }
        }
    } finally {
        isProcessingAck = false;
        if (ackQueue.length > 0) runAckQueue(sock);
    }
}

// Procesador de EDICIÓN (Búsqueda y Resultado)
async function runProcessor(sock) {
    if (isProcessingQueue || processingQueue.length === 0) return;
    isProcessingQueue = true;

    try {
        while (processingQueue.length > 0) {
            const { sentMsg, userMsg, remoteJid, idToFind } = processingQueue.shift();

            console.log(`[QUEUE] Procesando edición para ID: ${idToFind}. Pendientes en cola: ${processingQueue.length}`);

            try {
                // Delay 5s (Efecto búsqueda) - Aquí ocurre la pausa "escalera"
                await delay(5000);

                // Buscar en Caché
                const found = cachedCoordenadas.find(item => item.ID === idToFind);
                
                // Fecha dinámica
                const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
                const date = new Date();
                const formattedDate = `${date.getDate()} de ${months[date.getMonth()]}`;

                if (found) {
                    const lat = found.Latitud || found.lat || 'No definida';
                    const long = found.Longitud || found.long || 'No definida';
                    
                    // Texto FINAL (Reemplaza al anterior)
                    const finalText = `📍 *Coordenadas Encontradas* 📍\n\n🆔 *ID:* ${idToFind}\n🌍 *Latitud:* ${lat}\n🌍 *Longitud:* ${long}\n\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;

                    // EDITAR el mensaje anterior (Reemplazo total)
                    await sock.sendMessage(remoteJid, { 
                        text: finalText,
                        edit: sentMsg.key 
                    });
                    
                    // REACCIÓN al mensaje del USUARIO (userMsg.key)
                    await sock.sendMessage(remoteJid, { react: { text: '👨‍💻', key: userMsg.key } });
                    
                } else {
                    const notFoundText = `❌ No se encontraron coordenadas para el ID: ${idToFind}\n\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
                    
                    await sock.sendMessage(remoteJid, { 
                        text: notFoundText,
                        edit: sentMsg.key 
                    });
                }

                // Pequeña pausa entre tareas para no saturar
                await delay(500);

            } catch (err) {
                console.error(`[QUEUE] Error procesando ${idToFind}:`, err);
                // Si falla uno, continuamos con el siguiente. NO lanzamos error para no detener la cola.
            }
        }
    } finally {
        isProcessingQueue = false;
        // Si quedaron elementos por alguna razón (race condition), intentamos reiniciar
        if (processingQueue.length > 0) {
            console.log('[QUEUE] Cola no vacía tras finalizar, reiniciando procesador...');
            runProcessor(sock);
        }
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
    // Auth State
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
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

    // Manejo de Mensajes
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        for (const msg of messages) {
            try {
                if (!msg.message) continue;
                // Ignorar mensajes propios y mensajes de estado
                if (msg.key.fromMe) continue;
                if (msg.message.protocolMessage) continue;
                if (msg.message.reactionMessage) continue;

                // Extraer texto
                const msgType = Object.keys(msg.message)[0];
                const text = msgType === 'conversation' 
                    ? msg.message.conversation 
                    : msgType === 'extendedTextMessage' 
                        ? msg.message.extendedTextMessage.text 
                        : '';

                if (!text) continue;
                const remoteJid = msg.key.remoteJid;

                // --- ANTI-BUCLE / ANTI-SPAM ---
                // Solo ignoramos NUESTRAS PROPIAS RESPUESTAS para evitar bucles infinitos.
                // Permitimos "⚠️" y "COORDENADAS" porque son parte del input del usuario.
                if (text.includes('SOLICITUD ACEPTADA') ||
                    text.includes('Buscando 🔎') ||
                    text.includes('Cuadrilla 𝗕𝗼𝘁') ||
                    text.includes('Coordenadas Encontradas')) {
                    console.log(`[ANTI-BUCLE] Ignorando mensaje propio/repetido en ${remoteJid}`);
                    continue;
                }

                console.log(`Mensaje recibido de ${remoteJid}: ${text.substring(0, 50)}...`);

                // --- COMANDOS ---
                
                // Ping
                if (text === '.ping') {
                    console.log('Respondiendo a .ping');
                    await sock.readMessages([msg.key]); // Marcar leído (Blue check)
                    await sock.sendMessage(remoteJid, { text: 'pong!' }, { quoted: msg });
                }

                // Menu
                if (text === '.menuprincipal') {
                    console.log('Respondiendo a .menuprincipal');
                    await sock.sendMessage(remoteJid, { text: 'Bot Baileys Activo y Ligero ⚡' }, { quoted: msg });
                }

                // --- BUSCADOR MC ---
                const regex = /MC[:\s]*\d+/i;
                const match = text.match(regex);

                if (match) {
                    const idToFind = match[0].toUpperCase().replace(/[^A-Z0-9]/g, ''); // MC12345
                    const idNumbers = idToFind.replace('MC', '');

                    console.log(`[BUSCADOR] Solicitud: ${idToFind} de ${remoteJid}`);

                    if (idNumbers.length !== 5) {
                        await sock.sendMessage(remoteJid, { text: 'ID NO EXISTE VERIFICAR .' }, { quoted: msg });
                        continue;
                    }

                    // 1. AÑADIR A LA COLA DE ACK (Envío Inicial Seguro)
                    // Ya no enviamos directamente con await sock.sendMessage() aquí para evitar rate-limit
                    ackQueue.push({ msg, remoteJid, idToFind, idNumbers });
                    runAckQueue(sock);
                }
            } catch (err) {
                console.error('Error procesando mensaje:', err);
            }
        }
    });
}

startBot().catch(err => console.error('Error al iniciar bot:', err));