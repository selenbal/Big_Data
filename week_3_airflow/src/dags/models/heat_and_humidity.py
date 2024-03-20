class HeatAndHumidityMeasureEvent():
    def __init__(self, temperature, humidity, timestamp, creator):
        self.temperature = temperature
        self.humidity = humidity
        self.timestamp = timestamp
        self.creator=creator

    def __str__(self):
        return f"Temperature: {self.temperature}, Humidity: {self.humidity}, Timestamp: {self.timestamp}"