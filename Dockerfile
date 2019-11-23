FROM golang:1.12-alpine

WORKDIR /go/src/app
RUN apk update && apk add git
# RUN go get -u github.com/kataras/iris
# Breaking change: https://github.com/kataras/iris/issues/1385#issuecomment-546643215
RUN go get github.com/iris-contrib/cloud-native-go
COPY ./src .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["app"]
