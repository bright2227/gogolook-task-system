FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN go mod tidy
ARG SERVICE=server
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/service ./cmd/${SERVICE}
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/taskctl ./cmd/taskctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
COPY --from=build /out/taskctl /taskctl
USER nonroot:nonroot
ENTRYPOINT ["/service"]
