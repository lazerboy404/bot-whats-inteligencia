FROM node:18-slim

# Instalamos git por si acaso, pero NO instalamos chrome/puppeteer para ahorrar espacio
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

CMD [ "npm", "start" ]
