## Desicion de implementacion

Para la implmenetacion del middelware cree una clase auxiliar _BaseRabbitMQMiddleware() para encapsular ahi el codigo repetido y poder reutilizarlo.

MessageMiddlewareQueueRabbitMQ() y MessageMiddlewareExchangeRabbitMQ() usan herncia multiple, pytohn lee las clases padre de izquierda a derecha.
