# builder 源镜像
FROM golang:1.12.4-alpine as builder

# 安装git
RUN apk add --no-cache git

WORKDIR /app

COPY . .

RUN go mod download && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

# prod 源镜像
FROM alpine:latest as prod

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=0 /app/DL .

EXPOSE 9202
ENTRYPOINT ["/app/DL"]
