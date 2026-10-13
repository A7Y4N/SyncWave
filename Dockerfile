FROM node:20-alpine

# Install ffmpeg for real-time audio file decode
RUN apk add --no-cache ffmpeg

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY public ./public

ENV PORT=8080
EXPOSE 8080

CMD ["npm", "start"]
