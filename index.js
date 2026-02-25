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

// --- COLA DE MENSAJES (QUEUE) ---
// Procesamiento secuencial para efecto "escalera"
const messageQueue = [];
let isProcessingQueue = false;

async function processQueue(sock) {
    if (isProcessingQueue || messageQueue.length === 0) return;
    isProcessingQueue = true;

    while (messageQueue.length > 0) {
        const { msg, remoteJid, idToFind, idNumbers } = messageQueue.shift();

        try {
            console.log(`[QUEUE] Procesando ID: ${idToFind} para ${remoteJid}`);

            // 1. Mensaje de Aceptación
            const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
            const date = new Date();
            const formattedDate = `${date.getDate()} de ${months[date.getMonth()]}`;

            const acceptanceText = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
            
            // Enviamos mensaje inicial
            const sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });

            // Delay 5s (Efecto búsqueda)
            await delay(5000);

            // Buscar en Caché
            const found = cachedCoordenadas.find(item => item.ID === idToFind);

            if (found) {
                const lat = found.Latitud || found.lat || 'No definida';
                const long = found.Longitud || found.long || 'No definida';
                const finalText = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}\n\n📍 *Coordenadas Encontradas* 📍\n\n🆔 *ID:* ${idToFind}\n🌍 *Latitud:* ${lat}\n🌍 *Longitud:* ${long}`;

                // Intentamos EDITAR el mensaje anterior para que aparezca todo en uno
                await sock.sendMessage(remoteJid, { 
                    text: finalText,
                    edit: sentMsg.key 
                });
                
                // Reacción final
                await sock.sendMessage(remoteJid, { react: { text: '👨‍💻', key: sentMsg.key } });
                
            } else {
                const notFoundText = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}\n\n❌ No se encontraron coordenadas para el ID: ${idToFind}`;
                
                await sock.sendMessage(remoteJid, { 
                    text: notFoundText,
                    edit: sentMsg.key 
                });
            }

            // Pequeña pausa entre mensajes de la cola para evitar saturación
            await delay(1000);

        } catch (err) {
            console.error(`[QUEUE] Error procesando ${idToFind}:`, err);
        }
    }

    isProcessingQueue = false;
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

                    // Simular Escribiendo
                    await sock.sendPresenceUpdate('composing', remoteJid);

                    // AÑADIR A LA COLA
                    messageQueue.push({ msg, remoteJid, idToFind, idNumbers });
                    processQueue(sock); // Iniciar procesamiento si no está corriendo
                }
            } catch (err) {
                console.error('Error procesando mensaje:', err);
            }
        }
    });
}

startBot().catch(err => console.error('Error al iniciar bot:', err));