const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    makeCacheableSignalKeyStore,
    fetchLatestBaileysVersion,
    delay,
    proto
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const express = require('express');
const qrcode = require('qrcode-terminal');
const fs = require('fs');

// --- CONFIGURACIÓN SERVIDOR (Render Keep-Alive) ---
const app = express();
const PORT = process.env.PORT || 3000;
let qrCodeData = null;
let cachedCoordenadas = []; // CACHÉ JSON

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
                </body>
            </html>
        `);
    }
});

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

    const sock = makeWASocket({
        version,
        logger: pino({ level: 'silent' }), // Silencioso para ahorrar logs
        printQRInTerminal: true, // QR en consola
        auth: {
            creds: state.creds,
            // CLAVE: makeCacheableSignalKeyStore evita el error "No sessions" en grupos
            keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
        },
        generateHighQualityLinkPreview: true,
        browser: ['Bot Baileys', 'Chrome', '1.0.0'], // Navegador falso
        syncFullHistory: false // Ahorra memoria al no sincronizar todo el historial antiguo
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
            console.log('Conexión cerrada. Reconectando:', shouldReconnect);
            if (shouldReconnect) {
                startBot();
            } else {
                console.log('Sesión cerrada. Borra la carpeta auth_info_baileys y reinicia.');
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
            if (!msg.message) continue;
            // Ignorar mensajes propios
            if (msg.key.fromMe) continue;

            // Extraer texto
            const msgType = Object.keys(msg.message)[0];
            const text = msgType === 'conversation' 
                ? msg.message.conversation 
                : msgType === 'extendedTextMessage' 
                    ? msg.message.extendedTextMessage.text 
                    : '';

            if (!text) continue;
            const remoteJid = msg.key.remoteJid;

            // --- COMANDOS ---
            
            // Ping
            if (text === '.ping') {
                await sock.readMessages([msg.key]); // Marcar leído (Blue check)
                await sock.sendMessage(remoteJid, { text: 'pong!' }, { quoted: msg });
            }

            // Menu
            if (text === '.menuprincipal') {
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

                // 1. Mensaje de Aceptación
                const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
                const date = new Date();
                const formattedDate = `${date.getDate()} de ${months[date.getMonth()]}`;

                const acceptanceText = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
                
                // Enviar mensaje inicial
                const sentMsg = await sock.sendMessage(remoteJid, { text: acceptanceText }, { quoted: msg });

                // Delay 5s
                await delay(5000);

                // Buscar en Caché
                const found = cachedCoordenadas.find(item => item.ID === idToFind);

                if (found) {
                    const lat = found.Latitud || found.lat || 'No definida';
                    const long = found.Longitud || found.long || 'No definida';
                    const finalText = `📍 *Coordenadas Encontradas* 📍\n\n🆔 *ID:* ${idToFind}\n🌍 *Latitud:* ${lat}\n🌍 *Longitud:* ${long}`;

                    // Reaccionar
                    await sock.sendMessage(remoteJid, { react: { text: '👨‍💻', key: msg.key } });

                    // IMPORTANTE: Baileys no permite "editar" mensajes de texto simple tan fácilmente como wwebjs en versiones antiguas,
                    // pero sí enviar uno nuevo citando al anterior o al original.
                    // Para mantener la consistencia con el pedido del usuario ("editar"), usaremos edit si es posible,
                    // pero en Baileys la edición tiene quirks. Lo más seguro es enviar la respuesta citando el mensaje de "Aceptada".
                    
                    // Opción A: Enviar nuevo mensaje con resultado (Más seguro en Baileys)
                    // Opción B: Intentar editar (protocolo edit). Probemos editar.
                    await sock.sendMessage(remoteJid, { 
                        text: finalText,
                        edit: sentMsg.key 
                    });
                    
                } else {
                    await sock.sendMessage(remoteJid, { 
                        text: `❌ No se encontraron coordenadas para el ID: ${idToFind}`,
                        edit: sentMsg.key 
                    });
                }
            }
        }
    });
}

startBot().catch(err => console.error('Error al iniciar bot:', err));