FROM golang:1.26-alpine AS build
WORKDIR /src
# Контекст - каталог services/: stub входит в модуль cbl-reporting и использует
# его replace-зависимости (cbl-protos, cbl-kit).
COPY cbl-kit/go.mod cbl-kit/go.sum ./cbl-kit/
COPY cbl-protos/go.mod cbl-protos/go.sum ./cbl-protos/
COPY cbl-reporting/go.mod cbl-reporting/go.sum ./cbl-reporting/
RUN cd cbl-reporting && go mod download
COPY cbl-kit ./cbl-kit
COPY cbl-protos ./cbl-protos
COPY cbl-reporting ./cbl-reporting
RUN cd cbl-reporting && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ledgerstub ./test/e2e/ledgerstub

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledgerstub /ledgerstub
EXPOSE 9040 9041
ENTRYPOINT ["/ledgerstub"]
