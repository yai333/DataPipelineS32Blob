FROM alpine AS azcopy
RUN apk add --no-cache wget \
    &&	wget https://aka.ms/downloadazcopy-v10-linux -O /tmp/azcopy.tgz \
    &&	export BIN_LOCATION=$(tar -tzf /tmp/azcopy.tgz | grep "/azcopy") \
    &&	tar -xzf /tmp/azcopy.tgz $BIN_LOCATION --strip-components=1 -C /usr/bin

FROM alpine:3.9
RUN apk update && apk add libc6-compat ca-certificates jq curl
COPY --from=azcopy /usr/bin/azcopy /usr/local/bin/azcopy
RUN ldd /usr/local/bin/azcopy
COPY entrypoint.sh  /
RUN chmod 777 /entrypoint.sh

ENTRYPOINT ["sh", "/entrypoint.sh"]