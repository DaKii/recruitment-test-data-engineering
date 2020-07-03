FROM node:14.4

WORKDIR /usr/src/node

COPY package.json ./
RUN npm install

COPY example.js ./
ENTRYPOINT ./example.js
