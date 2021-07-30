FROM golang:1.16.3-alpine AS build
WORKDIR /go/src/powerlog
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/powerlog ./cmd/powerlog

FROM scratch
COPY --from=build /go/bin/powerlog /bin/powerlog
ENTRYPOINT ["/bin/powerlog"]