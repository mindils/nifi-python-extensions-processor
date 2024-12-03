from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import re
import json
import avro.schema
import avro.io
import io


class ConvertRecordKeysToCaseWithRegex(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '1.1.5'
        description = 'Приводит ключи записей JSON и Avro к нижнему или верхнему регистру с использованием регулярных выражений. Данные Avro временно преобразуются в JSON для обработки.'
        tags = ['json', 'avro', 'case', 'keys', 'regex', 'record', 'lowercase', 'uppercase']
        dependencies = ['avro']  # Указываем зависимость для Avro

    def __init__(self, **kwargs):
        super().__init__()  # Вызываем конструктор базового класса
        self.convert_case = PropertyDescriptor(
            name="Convert to Lower Case",
            description="Укажите 'true' для преобразования ключей в нижний регистр, 'false' для преобразования в верхний регистр.",
            default_value="true",
            required=True,
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )
        self.descriptors = [self.convert_case]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, record, schema, attributemap):
        # Получаем настройку из свойств
        convert_to_lower_case = context.getProperty("Convert to Lower Case").getValue()
        to_lower = convert_to_lower_case.lower() == 'true'

        # Проверяем, является ли запись JSON или Avro
        if isinstance(record, dict):
            # Обработка JSON
            return self.process_json_record(record, to_lower)
        elif isinstance(record, bytes):
            # Обработка Avro
            schema_str = attributemap.getAttribute('avro.schema')
            if not schema_str:
                raise Exception('Схема Avro должна быть указана в атрибуте avro.schema')
            return self.process_avro_record(record, schema_str, to_lower)
        else:
            raise Exception(f'Неподдерживаемый формат данных: {type(record)}')

    def process_json_record(self, record, to_lower):
        """
        Преобразование ключей JSON с использованием регулярных выражений.
        """
        # Сериализуем словарь в строку JSON
        content_str = json.dumps(record)

        # Используем регулярное выражение для изменения ключей
        def replace_key(match):
            key = match.group(1)
            new_key = key.lower() if to_lower else key.upper()
            return f'"{new_key}":'

        new_content_str = re.sub(r'"([^"]+)":', replace_key, content_str)

        # Десериализуем обратно в Python-объект
        new_record = json.loads(new_content_str)

        # Возвращаем преобразованный результат
        return RecordTransformResult(
            record=new_record
        )

    def process_avro_record(self, avro_bytes, schema_str, to_lower):
        """
        Преобразует Avro запись: декодирует её, изменяет ключи, и возвращает обратно в Avro формате.
        """
        # Загружаем схему
        schema = avro.schema.parse(schema_str)

        # Декодируем Avro данные
        bytes_reader = io.BytesIO(avro_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        data = reader.read(decoder)

        # Преобразуем данные в строку JSON
        content_str = json.dumps(data)

        # Используем регулярное выражение для изменения ключей
        def replace_key(match):
            key = match.group(1)
            new_key = key.lower() if to_lower else key.upper()
            return f'"{new_key}":'

        new_content_str = re.sub(r'"([^"]+)":', replace_key, content_str)

        # Десериализуем обратно в Python-объект
        new_data = json.loads(new_content_str)

        # Кодируем данные обратно в Avro
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = avro.io.DatumWriter(schema)
        writer.write(new_data, encoder)

        return RecordTransformResult(
            record=bytes_writer.getvalue()
        )
