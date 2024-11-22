FROM alpine:3.14

RUN apk --no-cache add curl

COPY init-elasticsearch.sh /

ENTRYPOINT ["/init-elasticsearch.sh"]
