# Use OpenJDK 21 for the runtime
FROM openjdk:21-slim

# Set the working directory
WORKDIR /app

# Copy the fat JAR from the build stage (assuming it's already built)
COPY target/hello-world-1.0-SNAPSHOT.jar /app/kafka-proxy.jar

# Copy the default configuration file
COPY proxy.properties /app/proxy.properties

# Expose the proxy port
EXPOSE 9092

# Run the proxy
CMD ["java", "-jar", "kafka-proxy.jar"]
