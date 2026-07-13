JAR     = target\producer2-1.0-SNAPSHOT-jar-with-dependencies.jar
COMPOSE = src\main\docker\docker-compose.yml

.PHONY: build run stop logs clean

## Build fat JAR (bỏ qua test)
build:
	mvn clean package -DskipTests -q

## Build + khởi động Docker + chạy Consumer rồi Producer
run: build
	docker compose -f $(COMPOSE) up -d
	@echo Waiting for Kafka and MySQL to be ready (30s)...
	timeout /t 30 /nobreak > nul
	start "Kafka Consumer" java -cp $(JAR) org.example.Consumer
	timeout /t 2 /nobreak > nul
	java -cp $(JAR) org.example.Producer

## Dừng và xóa toàn bộ containers + volumes
stop:
	docker compose -f $(COMPOSE) down -v

## Xem log của các Docker service
logs:
	docker compose -f $(COMPOSE) logs -f

## Dừng Docker + xóa build artifacts
clean: stop
	mvn clean -q
