# Placeholder image to allow the infrastructure to build successfully
FROM alpine:latest

# Install curl so the Docker healthcheck command doesn't fail
RUN apk add --no-cache curl

# Use a tiny loop with netcat (nc) to fake a continuous HTTP 200 OK response on port 3000
CMD while true; do echo -e "HTTP/1.1 200 OK\n\nOK" | nc -l -p 3000; done