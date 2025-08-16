# ✅ Official Eclipse Mosquitto image
FROM eclipse-mosquitto:2.0.18

# ✅ Create necessary directories with correct ownership
RUN mkdir -p /mosquitto/config /mosquitto/data /mosquitto/log && \
    chmod -R 755 /mosquitto

# ✅ Copy config file (No ACL file needed)
COPY mosquitto.conf /mosquitto/config/mosquitto.conf

# ✅ Set proper permissions inside the container
RUN chmod 644 /mosquitto/config/mosquitto.conf

# ✅ Expose MQTT port
EXPOSE 1883

# ✅ Set Mosquitto user before running
USER mosquitto

# ✅ Start Mosquitto with config file
CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
