FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY src/PartnerSimulator.java .
RUN javac -d /out PartnerSimulator.java

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /out /app
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=50", "-cp", "/app", "PartnerSimulator"]
