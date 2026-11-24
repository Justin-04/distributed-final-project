FROM nginx:1.27-alpine

COPY gateway.common.conf gateway.server.conf gateway.status.conf /etc/nginx/gateway/
RUN mkdir -p /var/log/gateway
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
