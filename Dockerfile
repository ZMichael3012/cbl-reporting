FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reporting ./cmd/reporting

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/reporting /app/reporting
EXPOSE 8110 9110 9310
ENTRYPOINT ["/app/reporting"]