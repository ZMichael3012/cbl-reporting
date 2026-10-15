FROM golang:1.26-alpine AS build
WORKDIR /src
# Контекст - каталог services/: replace ../cbl-kit и ../cbl-protos требует соседние модули.
COPY cbl-kit/go.mod cbl-kit/go.sum ./cbl-kit/
COPY cbl-protos/go.mod cbl-protos/go.sum ./cbl-protos/
COPY cbl-reporting/go.mod cbl-reporting/go.sum ./cbl-reporting/
RUN cd cbl-reporting && go mod download
COPY cbl-kit ./cbl-kit
COPY cbl-protos ./cbl-protos
COPY cbl-reporting ./cbl-reporting
RUN cd cbl-reporting && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reporting ./cmd/reporting

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/reporting /app/reporting
# Миграции применяются сервисом при старте (MIGRATIONS_DIR).
COPY --from=build /src/cbl-reporting/migrations /app/migrations
EXPOSE 8110 9110 9310
ENTRYPOINT ["/app/reporting"]
