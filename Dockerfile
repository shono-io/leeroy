FROM golang:1.20 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 leeroy

WORKDIR /go/src/github.com/shono-io/leeroy
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY . /go/src/github.com/shono-io/leeroy
RUN rm -rf /go/src/github.com/shono-io/leeroy/go.sum
RUN go mod tidy
RUN go mod download

# Build
RUN go build -tags "timetzdata" -ldflags "-w -s" -o leeroy

# Pack
FROM busybox AS package

LABEL maintainer="Daan Gerits <daan@shono.io>"

ENV stage local
ENV config_name app.yaml

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/shono-io/leeroy/leeroy .

RUN mkdir /config

USER leeroy

EXPOSE 4195

ENTRYPOINT ["/leeroy"]

CMD ["-c", "benthos.yaml"]