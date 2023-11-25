FROM node:18-alpine as base

WORKDIR /src
COPY package*.json /
EXPOSE 3000

FROM base as production
ENV NODE_ENV=production
ENV NODE_OPTIONS=--max_old_space_size=2048
RUN npm ci
COPY . /
CMD ["node", "--max-old-space-size=4094", "index.js"]