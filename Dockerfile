# builder 源镜像
FROM golang:1.12.4-alpine as builder

# 安装git
RUN apk add --no-cache git

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go build

# prod 源镜像
FROM alpine:latest as prod

RUN apk --no-cache add ca-certificates

ENV DL_PORT=9201 ES_HOST=127.0.0.1 ES_PORT=9200 BP_LOG_OUTPUT=./dl.log BP_LOG_LEVEL=WARN

WORKDIR /app

COPY --from=0 /app/DL .

EXPOSE 9202
ENTRYPOINT ["/app/DL"]
