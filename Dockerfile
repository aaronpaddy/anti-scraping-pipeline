# Builds every Go command (engine, generator, seed-export, eval) into one image.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
COPY --from=build /out/ /usr/local/bin/
WORKDIR /work
