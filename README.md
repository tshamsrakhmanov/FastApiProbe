**_Стек_**\
python, counfluentic_kafka(py),docker

**_Запуск, использование_**\
Всё стартует с помощью docker compose\
(сборка фаст апи, запуск кафки)\
Кафка сконфигурирована с полным доступом по портам "снаружи" докера - порты проброшены.\
После старта нужно скормить команду - создаст топик\
```docker exec -it ipr2_kafka_fastapi_jmeter-kafka1-1 kafka-topics --create --topic test1 --bootstrap-server kafka1:9090 --replication-factor 1 --partitions 1```\
Еще команда: для листинга сообщений по топику реал тайм:\
```docker exec -it ipr2_kafka_fastapi_jmeter-kafka1-1 kafka-console-consumer --bootstrap-server kafka1:9090 --topic test1 --from-beginning```

**_Дополнительно_**\
Снабжён jmeter скриптом для отправки\считки сообщений.


**_\\ Timur Shamsrakhmanov \\_** \
**_\\ december 2024  \\_**