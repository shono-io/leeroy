FROM golang:1.21 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 leeroy

WORKDIR /go/src/github.com/shono-io/leeroy/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/shono-io/leeroy/
RUN go mod download

# Build
COPY ../.. /go/src/github.com/shono-io/leeroy/
# Tag timetzdata required for busybox base image:
# https://github.com/benthosdev/benthos/issues/897
RUN make TAGS="timetzdata"

# Pack
FROM busybox AS package

LABEL maintainer="Daan Gerits <daan@shono.io>"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/shono-io/leeroy/target/bin/leeroy .

USER benthos

EXPOSE 4195

ENTRYPOINT ["/leeroy"]

CMD ["-c", "/benthos.yaml"]
