Простой проект на базе FastAPI и Confluentic_Kafka для примера взаимодействия.

Проект может быть запущен и сконфигурирован как отдельно (например в среде разработки) так и в виде отдельного докер-контейнера.

Во втором случае требуется дополнительно указать сеть внутри докера (т.к. кафка будет запущена посредством compose файла и будет иметь свою отдельную под-сеть) и указать сокет требуемого кафка брокера.
Для сборки в докере - представлен докер файл. Сборка стандартным методом.

В составе проекта также есть docker compose с самой кафкой - для удобства.

upd:
всё стартует с помощью docker compose
после старта нужно скормить команду - создаст топик
```docker exec -it ipr2_kafka_fastapi_jmeter-kafka1-1 kafka-topics --create --topic test1 --bootstrap-server kafka1:9090 --replication-factor 1 --partitions 1```
еще команда: для листинка сообщений по топику реал тайм:
```docker exec -it ipr2_kafka_fastapi_jmeter-kafka1-1 kafka-console-consumer --bootstrap-server kafka1:9090 --topic test1 --from-beginning```