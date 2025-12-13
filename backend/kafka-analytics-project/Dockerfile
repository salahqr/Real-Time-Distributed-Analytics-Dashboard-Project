FROM eclipse-temurin:21-jdk-jammy

WORKDIR /app

COPY Kafka-Project/target/*.jar /app/app.jar

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]