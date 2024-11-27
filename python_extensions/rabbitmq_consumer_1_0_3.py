from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

class RabbitMQConsumer(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.3'
        description = 'Получает сообщения из RabbitMQ и передает их дальше'
        dependencies = ['pika']

    def __init__(self, **kwargs):

        # Определение свойств процессора
        self.HOST = PropertyDescriptor(
            name='RabbitMQ Host',
            description='Адрес сервера RabbitMQ',
            required=True,
            default_value='localhost',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.PORT = PropertyDescriptor(
            name='RabbitMQ Port',
            description='Порт RabbitMQ',
            required=True,
            default_value='5672',
            validators=[StandardValidators.PORT_VALIDATOR]
        )

        self.USERNAME = PropertyDescriptor(
            name='RabbitMQ Username',
            description='Имя пользователя RabbitMQ',
            required=True,
            default_value='guest',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.PASSWORD = PropertyDescriptor(
            name='RabbitMQ Password',
            description='Пароль RabbitMQ',
            required=True,
            sensitive=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.VHOST = PropertyDescriptor(
            name='RabbitMQ Virtual Host',
            description='Виртуальный хост RabbitMQ',
            required=True,
            default_value='/',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.PRIORITY_QUEUE = PropertyDescriptor(
            name='Priority Queue Name',
            description='Имя приоритетной очереди',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.NORMAL_QUEUE = PropertyDescriptor(
            name='Normal Queue Name',
            description='Имя обычной очереди',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.descriptors = [
            self.HOST,
            self.PORT,
            self.USERNAME,
            self.PASSWORD,
            self.VHOST,
            self.PRIORITY_QUEUE,
            self.NORMAL_QUEUE
        ]

        self.relationships = {
            Relationship(name='success', description='Успешно обработанные FlowFiles'),
            Relationship(name='no_message', description='FlowFiles без сообщений в очередях')
        }

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        return self.relationships

    def transform(self, context, flowFile):
        import pika

        # Получение значений свойств
        host = context.getProperty(self.HOST.name).getValue()
        port = int(context.getProperty(self.PORT.name).getValue())
        username = context.getProperty(self.USERNAME.name).getValue()
        password = context.getProperty(self.PASSWORD.name).getValue()
        virtual_host = context.getProperty(self.VHOST.name).getValue()
        priority_queue = context.getProperty(self.PRIORITY_QUEUE.name).getValue()
        normal_queue = context.getProperty(self.NORMAL_QUEUE.name).getValue()

        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials
        )

        try:
            # Установка соединения и открытие канала
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Попытка получить сообщение из приоритетной очереди
            method_frame, header_frame, body = channel.basic_get(queue=priority_queue, auto_ack=False)
            if method_frame is None:
                # Если в приоритетной очереди нет сообщений, пытаемся получить из обычной очереди
                method_frame, header_frame, body = channel.basic_get(queue=normal_queue, auto_ack=False)
                if method_frame is None:
                    # Если сообщений нет ни в одной из очередей, отправляем FlowFile в связь 'no_message'
                    channel.close()
                    connection.close()
                    return FlowFileTransformResult(relationship='no_message')

            # Подтверждаем получение сообщения
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            # Закрываем соединение
            channel.close()
            connection.close()

            # Возвращаем содержимое сообщения как новое содержимое FlowFile
            return FlowFileTransformResult(relationship='success', contents=body)
        except Exception as e:
            self.logger.error('Ошибка при получении сообщения из RabbitMQ: {}'.format(str(e)))
            # В случае ошибки отправляем FlowFile в связь 'no_message'
            return FlowFileTransformResult(relationship='no_message')
