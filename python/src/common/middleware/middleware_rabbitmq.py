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


    def start_consuming(self, on_message_callback):
        def _callback(ch, method, properties, body):
            state = {'called': False}

            def ack():
                if not state['called']:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    state['called'] = True

            def nack():
                if not state['called']:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    state['called'] = True

            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error en callback de usuario: {e}") from e

        try:
            self._is_consuming = True
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_callback,
                auto_ack=False
            )
            self._channel.start_consuming()
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker) as e:
            self._is_consuming = False
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al consumir: {e}") from e
        except Exception as e:
            self._is_consuming = False
            raise MessageMiddlewareMessageError(f"Error al consumir: {e}") from e


    def stop_consuming(self):
        try:
            if hasattr(self, '_channel') and self._channel and self._channel.is_open and self._is_consuming:
                if self._consumer_tag:
                    self._channel.basic_cancel(self._consumer_tag)
                    self._consumer_tag = None
                self._channel.stop_consuming()
                self._is_consuming = False
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexión perdida al detener consumo: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al detener consumo: {e}") from e


    def close(self):
        try:
            self.stop_consuming()
            if hasattr(self, '_channel') and self._channel and self._channel.is_open:
                self._channel.close()
            if hasattr(self, '_connection') and self._connection and self._connection.is_open:
                self._connection.close()
        except (AMQPConnectionError, StreamLostError):
            pass
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar middleware: {e}") from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
