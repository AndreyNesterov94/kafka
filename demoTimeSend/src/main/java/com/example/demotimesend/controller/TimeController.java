package com.example.demotimesend.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

@RestController
public class TimeController {

    private static final String TOPIC = "time-topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/send-time")
    public ResponseEntity<String> sendTime(@RequestParam("requestTime") String requestTime) {
        try {
            // Получаем текущее реальное время
            LocalTime currentTime = LocalTime.now();

            // Преобразуем время запроса из строки в объект LocalTime
            LocalTime parsedRequestTime = LocalTime.parse(requestTime);

            // Вычисляем разницу между текущим временем и временем запроса
            Duration timeDifference = Duration.between(parsedRequestTime, currentTime);

            // Формируем сообщение для отправки в топик Kafka
            String message = "Текущее время: " + currentTime.toString() + ", Время запроса: " + requestTime + ", Разница: " + timeDifference.getSeconds() + " секунд";

            // Отправляем сообщение в топик Kafka
            kafkaTemplate.send(TOPIC, message);

            // Возвращаем сообщение об успешной отправке времени
            return ResponseEntity.ok("Сообщение отправлено в топик Kafka: " + message);
        } catch (Exception e) {
            // Обрабатываем ошибки при отправке времени в Kafka
            return ResponseEntity.internalServerError().body("Ошибка при отправке сообщения в топик Kafka: " + e.getMessage());
        }
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<String> handleHttpMessageNotReadableException(HttpMessageNotReadableException e) {
        // Обрабатываем ошибки, связанные с неверным форматом запроса
        return ResponseEntity.badRequest().body("Неверный формат запроса: " + e.getMessage());
    }

}
