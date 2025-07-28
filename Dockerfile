# Dockerfile for the Go Workflow Dispatcher application

# --- Stage 1: Build ---
# This stage uses a specific version of the Go image to build the application.
# Using a specific version ensures reproducible builds.
FROM golang:1.24-alpine AS builder

# Set the working directory inside the container.
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies first.
# This leverages Docker's layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code.
COPY . .

# Build the Go application.
# -ldflags="-w -s" strips debugging information, reducing the binary size.
# CGO_ENABLED=0 and GOOS=linux are set for cross-compilation and static linking.
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-w -s" -o /app/main .

# --- Stage 2: Final Image ---
# This stage uses a minimal "alpine" image. Unlike "scratch", alpine includes
# essential libraries and, crucially, the trusted root CA certificates
# needed for making secure HTTPS (TLS) connections.
FROM alpine:latest

# Set the working directory.
WORKDIR /app

# Copy the compiled binary from the 'builder' stage.
COPY --from=builder /app/main .

# The port the application will listen on.
# This does not publish the port, but serves as documentation for Cloud Run.
EXPOSE 8080

# The command to run when the container starts.
ENTRYPOINT ["/app/main"]
