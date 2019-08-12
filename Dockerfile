#源镜像
FROM    golang:1.12.4-alpine

# 安装git
RUN     apk add --no-cache git

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

EXPOSE 9201
ENTRYPOINT ["/app/DL"]
