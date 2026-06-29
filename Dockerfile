# --- build stage -------------------------------------------------------------
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads separately from source for faster rebuilds.
COPY go.mod ./
# go.sum is added once the project has external dependencies (Phase 1).
RUN go mod download || true

COPY . .
# CGO disabled -> a static binary that runs in a scratch/distroless image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# --- run stage ---------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/server"]
