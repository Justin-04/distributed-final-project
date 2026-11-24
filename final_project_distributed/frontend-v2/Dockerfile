FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
# npm can crash ("Exit handler never called") yet exit 0, leaving node_modules incomplete;
# the explicit check makes such a failure stop the build here instead of later.
RUN npm ci --no-audit --no-fund && test -x node_modules/.bin/vite

COPY . .

# local binary, not `npx`: npx would silently try to download vite if it were missing
RUN ./node_modules/.bin/vite build

FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html

COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]