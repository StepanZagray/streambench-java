FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build
COPY src/ ./src/
RUN javac --release 21 --add-modules jdk.httpserver -d classes src/*.java

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/classes ./classes
COPY data/events.jsonl ./data/events.jsonl
ENV PORT=8080 DATA_DIR=/app/data MAX_STREAMS=32
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "--add-modules", "jdk.httpserver", "-cp", "/app/classes", "Streambench"]
