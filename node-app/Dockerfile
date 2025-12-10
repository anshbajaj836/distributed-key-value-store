# stage 1: Build the code
FROM maven:3.8-openjdk-17 AS BUILDER

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Stage 2 create runtime image
FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

# we copy the jar that includes dependencies
COPY --from=BUILDER /app/target/*-jar-with-dependencies.jar app.jar
EXPOSE 7000
ENTRYPOINT ["java","-jar","app.jar"]
