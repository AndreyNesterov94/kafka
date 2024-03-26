package com.example.demotimelistener.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;

@Component
public class TimeController {

    private static final String FILE_NAME = "/root/ms2/time_differences.txt"; // Абсолютный путь к файлу

    @KafkaListener(topics = "time-topic", groupId = "time-consumer-group")
    public void consume(ConsumerRecord<String, String> record) {
        String message = record.value();
        // Извлекаем первое время из сообщения Kafka
        String[] parts = message.split(",");
        String firstTimeStr = parts[0].trim().replace("Текущее время: ", ""); // Берем первое время из строки и удаляем "Текущее время: "
        // Преобразуем первое время из строки в объект LocalTime
        LocalTime firstTime = LocalTime.parse(firstTimeStr);
        // Получаем текущее системное время
        LocalTime currentTime = LocalTime.now();
        // Вычисляем разницу в миллисекундах
        long timeDifferenceMillis = Duration.between(firstTime, currentTime).toMillis();
        // Сохраняем разницу в файл
        saveToFile(Long.toString(timeDifferenceMillis));
    }

    private void saveToFile(String value) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME, true))) {
            writer.write(value);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
