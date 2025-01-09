FROM node:lts-alpine

# Create app directory
WORKDIR /app

COPY . /app

RUN npm install

EXPOSE 9001
#CMD [ "node", "index.js" ]
CMD ["node", "index.js"]
