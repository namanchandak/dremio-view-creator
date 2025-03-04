FROM node:20
WORKDIR /app
COPY . /app
RUN npm i --legacy-peer-deps
EXPOSE 5555
CMD node src/index.js