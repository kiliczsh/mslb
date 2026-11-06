FROM postgres:17-alpine@sha256:562a6e0a66b5136bc28bef5944bbc2ddddf8ea0058d4016b96cee63f2edaf143

# initdb imports available collations through locale(1), absent from the base image.
RUN apk add --no-cache musl-locales=0.1.0-r1 libintl=1.0-r0
