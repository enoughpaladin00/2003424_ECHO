# Use a lightweight Node.js Alpine image
FROM node:20-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy package files
COPY package*.json ./

# CHANGE HERE: Use npm install instead of npm ci during development
RUN npm install

# Copy the rest of the frontend source code
COPY . .

# Expose port 3000 to match the Nginx upstream configuration
EXPOSE 3000

# Start the React development server
CMD ["npm", "start"]