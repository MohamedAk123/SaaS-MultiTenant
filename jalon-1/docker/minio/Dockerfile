# Official binary images are no longer available at the tested registries.
# Build the upstream tagged source, retaining the web console for this local lab.
FROM golang:1.24.2-alpine3.21 AS build
RUN apk add --no-cache git ca-certificates
ARG MINIO_TAG=RELEASE.2025-04-22T22-12-26Z
RUN git clone --depth 1 --branch ${MINIO_TAG} https://github.com/minio/minio.git /src
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -p 2 -trimpath -ldflags='-s -w' -o /minio .
FROM alpine:3.21.3
RUN apk add --no-cache ca-certificates curl && adduser -D -u 1000 minio && mkdir /data && chown minio:minio /data
COPY --from=build /minio /usr/local/bin/minio
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
