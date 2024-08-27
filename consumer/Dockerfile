FROM rust:1.75

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

CMD ["./target/release/consumer"]