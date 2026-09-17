import pika
from pika.exceptions import AMQPConnectionError, StreamLostError, ConnectionClosedByBroker
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
        self._consumer_tag = None
        self._is_consuming = False

        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=False)

        except (AMQPConnectionError, StreamLostError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexion: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inicializando la cola: {e}") from e
        
    
    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message
            )
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al enviar: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje: {e}") from e



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
