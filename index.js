const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const fs = require('fs');
const path = require('path');

// --- SERVIDOR EXPRESS PARA KEEP-ALIVE Y QR (TRUCO RENDER) ---
const app = express();
const PORT = process.env.PORT || 3000;

let qrCodeData = null; // Variable para almacenar el código QR

app.get('/', (req, res) => {
    if (qrCodeData) {
        // Mostrar el QR como imagen generada por una API externa (Google Charts o similar)
        // Esto permite ver el QR directamente en el navegador
        res.send(`
            <html>
                <head>
                    <title>Bot WhatsApp - Escanear QR</title>
                    <meta http-equiv="refresh" content="10"> <!-- Auto-recargar cada 10s -->
                    <style>
                        body { font-family: sans-serif; text-align: center; padding: 50px; background-color: #f0f2f5; }
                        h1 { color: #128C7E; }
                        .qr-container { background: white; padding: 20px; display: inline-block; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
                        p { margin-top: 20px; color: #555; }
                        .btn { display: inline-block; margin-top: 20px; padding: 10px 20px; background: #d9534f; color: white; text-decoration: none; border-radius: 5px; }
                    </style>
                </head>
                <body>
                    <h1>🤖 Bot de WhatsApp</h1>
                    <div class="qr-container">
                        <h2>Escanea este código:</h2>
                        <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(qrCodeData)}" alt="QR Code">
                        <p style="font-size: 12px; color: #777; margin-top: 10px; word-break: break-all;">
                            <strong>Código Crudo (si la imagen falla, copia esto y usa un generador QR):</strong><br>
                            ${qrCodeData}
                        </p>
                    </div>
                    <p>Si ya lo escaneaste, espera unos segundos. Esta página se recargará sola.</p>
                </body>
            </html>
        `);
    } else {
        res.send(`
            <html>
                <head>
                    <title>Bot WhatsApp - Estado</title>
                    <style>
                        body { font-family: sans-serif; text-align: center; padding: 50px; background-color: #f0f2f5; }
                        h1 { color: #128C7E; }
                        p { color: #555; }
                        .btn { display: inline-block; margin-top: 20px; padding: 10px 20px; background: #d9534f; color: white; text-decoration: none; border-radius: 5px; }
                        .btn:hover { background: #c9302c; }
                    </style>
                </head>
                <body>
                    <h1>¡El bot está vivo y conectado! 🤖✅</h1>
                    <p>No hay código QR pendiente. El bot está listo para usarse.</p>
                </body>
            </html>
        `);
    }
});

// Ruta para forzar el reseteo de la sesión (SIMPLE)
app.get('/reset-session', async (req, res) => {
    // Solo mostramos mensaje, el reseteo real debe ser manual desde Render con "Clear Cache"
    // para evitar crashes por memoria.
    res.send('Para reiniciar completamente, usa la opción "Manual Deploy > Clear build cache & deploy" en el panel de Render.');
});

app.listen(PORT, () => {
    console.log(`Servidor Express escuchando en el puerto ${PORT}`);
});
// -------------------------------------------------------

// Configuración del cliente con LocalAuth y argumentos de Puppeteer OPTIMIZADOS para Render (Free Tier)
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process', 
            '--disable-gpu'
        ]
    }
});

// Cola de mensajes para procesar uno por uno (efecto escalera)
const messageQueue = [];
let isProcessing = false;

// Función para procesar la cola
async function processQueue() {
    // Si ya estamos procesando o la cola está vacía, no hacemos nada
    if (isProcessing || messageQueue.length === 0) return;

    isProcessing = true;
    const msg = messageQueue.shift();

    try {
        await handleMessage(msg);
    } catch (error) {
        console.error('Error procesando mensaje de la cola:', error);
    } finally {
        isProcessing = false;
        // Procesar el siguiente mensaje en la cola (recursividad asíncrona segura)
        processQueue();
    }
}

// Función con la lógica principal del mensaje
async function handleMessage(msg) {
    const text = msg.body;
    
    // Obtener el chat para simular "escribiendo"
    const chat = await msg.getChat();
    
    // Comandos Básicos
    if (text === '.ping') {
        await chat.sendStateTyping(); // Simular escribiendo
        // Pequeño delay opcional para naturalidad, aunque sendStateTyping ya indica actividad
        msg.reply('pong!');
        return;
    }

    if (text === '.menuprincipal') {
        await chat.sendStateTyping();
        msg.reply('¡Hola! El bot está funcionando a la perfección con Puppeteer.');
        return;
    }

    // Buscador de Coordenadas
    // Regex para detectar ID con formato MC seguido de números (ej. MC32366, MC: 32366, MC 32366)
    const regex = /MC[:\s]*\d+/i;
    const match = text.match(regex);

    if (match) {
        // Normalizar el ID eliminando espacios y dos puntos para obtener el formato estándar MC12345
        const idToFind = match[0].toUpperCase().replace(/[^A-Z0-9]/g, '');
        console.log(`[BUSCADOR] ID detectado: ${idToFind} (Original: ${match[0]}) en mensaje de ${msg.from}`);

        // Extraer solo la parte numérica para validar la longitud
        const idNumbers = idToFind.replace('MC', '');

        // Validar que tenga exactamente 5 dígitos
        if (idNumbers.length !== 5) {
            console.log(`[BUSCADOR] ID inválido (longitud ${idNumbers.length}): ${idToFind}`);
            await msg.reply('ID NO EXISTE VERIFICAR .').catch(err => console.error('Error enviando reply de error:', err));
            return;
        }

        try {
            // Simular 'escribiendo' mientras busca
            console.log('[DEBUG] Paso 1: Intentando sendStateTyping...');
            await chat.sendStateTyping();
            console.log('[DEBUG] Paso 2: sendStateTyping enviado correctamente');

            // 1. Enviar mensaje de "SOLICITUD ACEPTADA"
            const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
            const date = new Date();
            const day = date.getDate();
            const month = months[date.getMonth()];
            const formattedDate = `${day} de ${month}`;

            const acceptanceMessage = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
            
            console.log('[DEBUG] Paso 3: Intentando enviar mensaje de aceptación...');
            let sentMessage;
            try {
                sentMessage = await msg.reply(acceptanceMessage);
                console.log('[DEBUG] Paso 4: Mensaje de aceptación enviado (msg.reply)');
            } catch (replyError) {
                console.error('[DEBUG] Error en msg.reply, intentando sendMessage directo:', replyError);
                sentMessage = await client.sendMessage(msg.from, acceptanceMessage);
                console.log('[DEBUG] Paso 4b: Mensaje de aceptación enviado (client.sendMessage)');
            }

            if (!sentMessage) {
                throw new Error('No se pudo enviar el mensaje de aceptación');
            }

            // 2. Simular tiempo de búsqueda para mayor realismo
            // Volver a poner estado 'escribiendo' y esperar unos segundos
            console.log('[DEBUG] Paso 5: Iniciando delay de 5 segundos...');
            await chat.sendStateTyping().catch(e => console.error('Error en segundo sendStateTyping:', e));
            await new Promise(resolve => setTimeout(resolve, 5000)); // Espera de 5 segundos
            console.log('[DEBUG] Paso 6: Delay completado');

            console.log('[BUSCADOR] Descargando JSON...');
            const response = await fetch('https://raw.githubusercontent.com/lazerboy404/buscador-totems/main/coordenadas-script.json');
            
            if (!response.ok) throw new Error(`Error HTTP: ${response.status}`);
            
            const data = await response.json();
            console.log(`[BUSCADOR] JSON descargado. ${data.length} registros.`);

            // Buscar el ID exacto
            const found = data.find(item => item.ID === idToFind);

            if (found) {
                console.log(`[BUSCADOR] Encontrado: ${JSON.stringify(found)}`);
                const lat = found.Latitud || found.lat || found.latitude || 'No definida';
                const long = found.Longitud || found.long || found.longitude || 'No definida';
                
                const responseText = `📍 *Coordenadas Encontradas* 📍\n\n🆔 *ID:* ${idToFind}\n🌍 *Latitud:* ${lat}\n🌍 *Longitud:* ${long}`;

                // Responder editando el mensaje original de "SOLICITUD ACEPTADA"
                console.log('[DEBUG] Paso 7: Intentando editar mensaje...');
                await sentMessage.edit(responseText);
                console.log('[BUSCADOR] Respuesta editada.');

                // Reaccionar al mensaje original del usuario
                await msg.react('👨‍💻').catch(e => console.error('Error al reaccionar:', e));
                console.log('[BUSCADOR] Reacción enviada.');
            } else {
                console.log(`[BUSCADOR] ID ${idToFind} no encontrado en el JSON.`);
                await sentMessage.edit(`❌ No se encontraron coordenadas para el ID: ${idToFind}`);
            }
        } catch (error) {
            console.error('[BUSCADOR] Error general en proceso:', error);
            if (typeof sentMessage !== 'undefined') {
                await sentMessage.edit('Ocurrió un error al buscar las coordenadas. Por favor, intenta de nuevo más tarde.').catch(e => console.error('Error editando mensaje de error:', e));
            } else {
                await client.sendMessage(msg.from, 'Ocurrió un error al procesar tu solicitud.').catch(e => console.error('Error enviando mensaje de error:', e));
            }
        }
    }
}

// Evento QR: Generar y mostrar el código QR en la terminal y en la web
client.on('qr', (qr) => {
    console.log('--------------------------------------------------------------------------------');
    console.log('QR RECIBIDO (Copia el texto de abajo y pégalo en un generador QR):');
    console.log(qr);
    console.log('--------------------------------------------------------------------------------');
    
    qrCodeData = qr; // Guardar el QR para mostrarlo en la web
    qrcode.generate(qr, { small: true });
    console.log('Escanea el código QR con tu WhatsApp o entra a la URL del bot.');
});

// Evento Ready: El bot está listo
client.on('ready', () => {
    console.log('Bot conectado con Puppeteer exitosamente.');
    qrCodeData = null; // Limpiar el QR una vez conectado
});

// Evento de Autenticación Fallida: Forzar reinicio si falla
client.on('auth_failure', msg => {
    console.error('Fallo de autenticación', msg);
    qrCodeData = null;
});

// Evento de Desconexión
client.on('disconnected', (reason) => {
    console.log('Cliente desconectado', reason);
    qrCodeData = null;
    client.initialize(); // Reintentar conexión
});

// Manejo de mensajes: Añadir a la cola y procesar
client.on('message', async msg => {
    messageQueue.push(msg);
    processQueue();
});

// Inicializar el cliente
console.log('Iniciando cliente de WhatsApp...');
client.initialize().then(() => {
    console.log('Cliente inicializado correctamente (esperando eventos...)');
}).catch(err => {
    console.error('Error FATAL al inicializar cliente:', err);
});
