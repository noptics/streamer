FROM golang:1.12 as Builder

RUN mkdir -p /go/src/github.com/noptics/streamer
ADD . /go/src/github.com/noptics/streamer

WORKDIR /go/src/github.com/noptics/streamer

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o streamer

FROM alpine:3.9

RUN apk add --no-cache curl bash ca-certificates

COPY --from=builder /go/src/github.com/noptics/streamer/streamer /streamer

CMD ["/streamer"]