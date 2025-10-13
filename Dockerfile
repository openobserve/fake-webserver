FROM golang:1.23 AS builder 

RUN update-ca-certificates
# RUN apk update && apk add --no-cache git
# Create zinc user.
ENV USER=app
ENV GROUP=app
ENV UID=10001
ENV GID=10001
# See https://stackoverflow.com/a/55757473/12429735RUN
RUN groupadd --gid "${GID}" "${GROUP}"
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    --gid "${GID}" \
    "${USER}"
# Create default directories for persistent Zinc data used in final build stage.
# It follows the Linux filesystem hierarchy pattern
# https://tldp.org/LDP/Linux-Filesystem-Hierarchy/html/var.html
RUN mkdir -p /data
WORKDIR $GOPATH/src/app

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o app

FROM scratch
# Import the user and group files from the builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# Copy the ssl certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy our static executable.
COPY --from=builder  /go/src/app/app /app

# Create directories that can be used to keep Zinc data persistent along with host source or named volumes
COPY --from=builder --chown=app:app /data /data

# Use an unprivileged user.
USER app:app
# Port on which the service will be exposed.
EXPOSE 8080
# Run the zinc binary.
ENTRYPOINT ["/app"]
