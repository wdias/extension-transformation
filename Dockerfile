FROM golang:1.12-alpine

WORKDIR /go/src/app
RUN apk update && apk add git
RUN go get -u github.com/kataras/iris
COPY ./src .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["app"]
