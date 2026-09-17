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

class _BaseRabbitMQMiddleware:
    def __init__(self, host):
        self._host = host
        self._consumer_tag = None
        self._is_consuming = False
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
            self._channel = self._connection.channel()
        except (AMQPConnectionError, StreamLostError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexion: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inicializando el middleware: {e}") from e

    def _start_consuming(self, queue_name, on_message_callback):
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
                queue=queue_name,
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

    def _stop_consuming(self):
        try:
            if hasattr(self, '_channel') and self._channel and self._channel.is_open and self._is_consuming:
                if self._consumer_tag:
                    self._channel.basic_cancel(self._consumer_tag)
                    self._consumer_tag = None
                self._channel.stop_consuming()
                self._is_consuming = False
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al detener consumo: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al detener consumo: {e}") from e

    def _close(self):
        try:
            self._stop_consuming()
            if hasattr(self, '_channel') and self._channel and self._channel.is_open:
                self._channel.close()
            if hasattr(self, '_connection') and self._connection and self._connection.is_open:
                self._connection.close()
        except (AMQPConnectionError, StreamLostError):
            pass
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar middleware: {e}") from e


class MessageMiddlewareQueueRabbitMQ(_BaseRabbitMQMiddleware, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name

        try:
            self._channel.queue_declare(queue=self._queue_name, durable=False)
        except (AMQPConnectionError, StreamLostError) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al declarar cola: {e}") from e
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
        super()._start_consuming(self._queue_name, on_message_callback)

    def stop_consuming(self):
        super()._stop_consuming()

    def close(self):
        super()._close()


class MessageMiddlewareExchangeRabbitMQ(_BaseRabbitMQMiddleware, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys

        try:
            self._channel.exchange_declare(
                exchange=self._exchange_name, 
                exchange_type='direct', 
                durable=False
            )
            # Cola anonima exclusiva para este suscriptor
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue

            for rk in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=rk
                )

        except (AMQPConnectionError, StreamLostError) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al configurar exchange: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inicializando exchange: {e}") from e

    def send(self, message):
        try:
            for rk in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=rk,
                    body=message
                )
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexion perdida al enviar: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje: {e}") from e
        
    def start_consuming(self, on_message_callback):
        super()._start_consuming(self._queue_name, on_message_callback)

    def stop_consuming(self):
        super()._stop_consuming()

    def close(self):
        super()._close()
