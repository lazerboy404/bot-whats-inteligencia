const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');

// --- SERVIDOR EXPRESS PARA KEEP-ALIVE (TRUCO RENDER) ---
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
    res.send('¡El bot está vivo! 🤖');
});

app.listen(PORT, () => {
    console.log(`Servidor Express escuchando en el puerto ${PORT}`);
});
// -------------------------------------------------------

// Configuración del cliente con LocalAuth y argumentos de Puppeteer para estabilidad
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
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
            msg.reply('ID NO EXISTE VERIFICAR .');
            return;
        }

        // Simular 'escribiendo' mientras busca
        await chat.sendStateTyping();

        // 1. Enviar mensaje de "SOLICITUD ACEPTADA"
        const months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
        const date = new Date();
        const day = date.getDate();
        const month = months[date.getMonth()];
        const formattedDate = `${day} de ${month}`;

        const acceptanceMessage = `👨‍💻 \`\`\`SOLICITUD ACEPTADA\`\`\` 👨‍💻\n\n> Buscando 🔎\n> Cuadrilla 𝗕𝗼𝘁 👨‍💻 | ${formattedDate}`;
        const sentMessage = await msg.reply(acceptanceMessage);

        // 2. Simular tiempo de búsqueda para mayor realismo
        // Volver a poner estado 'escribiendo' y esperar unos segundos
        await chat.sendStateTyping();
        await new Promise(resolve => setTimeout(resolve, 5000)); // Espera de 5 segundos

        try {
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
                await sentMessage.edit(responseText);
                console.log('[BUSCADOR] Respuesta editada.');

                // Reaccionar al mensaje original del usuario
                await msg.react('👨‍💻');
                console.log('[BUSCADOR] Reacción enviada.');
            } else {
                console.log(`[BUSCADOR] ID ${idToFind} no encontrado en el JSON.`);
                await sentMessage.edit(`❌ No se encontraron coordenadas para el ID: ${idToFind}`);
            }
        } catch (error) {
            console.error('[BUSCADOR] Error:', error);
            await sentMessage.edit('Ocurrió un error al buscar las coordenadas. Por favor, intenta de nuevo más tarde.');
        }
    }
}

// Evento QR: Generar y mostrar el código QR en la terminal
client.on('qr', (qr) => {
    console.log('QR RECIBIDO', qr);
    qrcode.generate(qr, { small: true });
    console.log('Escanea el código QR con tu WhatsApp.');
});

// Evento Ready: El bot está listo
client.on('ready', () => {
    console.log('Bot conectado con Puppeteer exitosamente.');
});

// Manejo de mensajes: Añadir a la cola y procesar
client.on('message', async msg => {
    messageQueue.push(msg);
    processQueue();
});

// Inicializar el cliente
client.initialize();
