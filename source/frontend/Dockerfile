FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
# Usiamo start invece di build per vedere l'errore reale nel log
CMD ["npm", "start"]