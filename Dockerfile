# Use Node.js LTS version
FROM node:18-slim
 
# Create app directory
WORKDIR /usr/src/app
 
# Copy package files
COPY package*.json ./
 
# Install dependencies
RUN npm install
 
# Bundle app source
COPY . .
 
# Expose port (adjust if your app uses a different port)
EXPOSE 3000
 
# Command to run the app
CMD [ "node", "index.js"]