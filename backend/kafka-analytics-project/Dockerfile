# Stage 1: Build with Maven
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Copy the pom.xml and source code from your Spring Boot project
COPY /Kafka-Project/pom.xml .
COPY /Kafka-Project/src ./src

# Build the application (skip tests)
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Copy the JAR from the build stage
COPY --from=build /build/target/*.jar /app/app.jar

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]