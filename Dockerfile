# Етап 1: Збірка (Builder)
FROM rust:latest AS builder
WORKDIR /app

# Встановлюємо Protobuf Compiler, який потрібен для tonic-build
RUN apt-get update && apt-get install -y protobuf-compiler

# Копіюємо весь код проєкту
COPY . .

# Компілюємо релізну версію нашої ноди
RUN cargo build --release --bin cognitum-node

# Етап 2: Легкий фінальний образ (Runtime)
FROM debian:bookworm-slim

# Встановлюємо базові сертифікати та бібліотеки
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

# Переносимо скомпільований бінарник з першого етапу
COPY --from=builder /app/target/release/cognitum-node /usr/local/bin/

# Відкриваємо порт gRPC
EXPOSE 50051

# Запускаємо ноду
CMD ["cognitum-node"]