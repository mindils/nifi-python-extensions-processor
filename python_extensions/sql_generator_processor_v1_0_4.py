from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json
import re
from jsonpath_ng import parse
from nifiapi.properties import PropertyDescriptor, StandardValidators

class SQLGeneratorProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.3'
        description = 'Генерирует SQL-запросы, заменяя плейсхолдерами значения из JSON с использованием JSONPath. Позволяет выбрать, куда поместить результат.'
        tags = ['SQL', 'JSON', 'Template']
        dependencies = ['jsonpath-ng']

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        sql_template = PropertyDescriptor(
            name='SQL Template',
            description='Шаблон SQL с плейсхолдерами для замены на значения из JSON. Используйте JSONPath-выражения внутри ${...}.',
            required=True,
            default_value='',
        )

        # Создаем валидатор для Output Destination
        def output_destination_validator():
            def validate(subject, value, context):
                allowable_values = ['flowfile-content', 'flowfile-attribute']
                if value not in allowable_values:
                    return False, "Value must be one of {}".format(', '.join(allowable_values))
                return True, None
            return validate

        output_destination = PropertyDescriptor(
            name='Output Destination',
            description='Определяет, куда поместить сгенерированный SQL: в содержимое FlowFile или в атрибут.',
            required=True,
            default_value='flowfile-attribute',
            validators=[output_destination_validator()]
        )

        attribute_name = PropertyDescriptor(
            name='Attribute Name',
            description='Имя атрибута, в который будет помещен сгенерированный SQL, если выбрано размещение в атрибуте.',
            required=False,
            default_value='generated_sql',
        )

        return [sql_template, output_destination, attribute_name]

    def transform(self, context, flowfile):
        # Получаем шаблон SQL из свойств процессора
        sql_template_prop = context.getProperty('SQL Template')
        sql_template = sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()
        if not sql_template:
            self.logger.error('Свойство SQL Template не установлено.')
            return FlowFileTransformResult(relationship='failure')

        # Получаем настройку Output Destination
        output_destination_prop = context.getProperty('Output Destination')
        output_destination = output_destination_prop.evaluateAttributeExpressions(flowfile).getValue()
        if not output_destination:
            output_destination = 'flowfile-attribute'  # Значение по умолчанию

        # Если выбран атрибут, получаем имя атрибута
        if output_destination == 'flowfile-attribute':
            attribute_name_prop = context.getProperty('Attribute Name')
            attribute_name = attribute_name_prop.evaluateAttributeExpressions(flowfile).getValue()
            if not attribute_name:
                attribute_name = 'generated_sql'  # Значение по умолчанию

        # Читаем содержимое FlowFile и парсим JSON
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')
        try:
            json_data = json.loads(content_str)
        except Exception as e:
            self.logger.error('Не удалось распарсить JSON: {}'.format(e))
            return FlowFileTransformResult(relationship='failure')

        # Функция для замены плейсхолдеров
        def replace_placeholder(match):
            expression = match.group(1)
            try:
                value = self.evaluate_expression(expression, json_data)
                return value
            except Exception as e:
                self.logger.error('Ошибка при обработке выражения {}: {}'.format(expression, e))
                return ''

        # Заменяем плейсхолдеры в шаблоне SQL
        sql_generated = re.sub(r'\$\{([^}]+)\}', replace_placeholder, sql_template)

        if output_destination == 'flowfile-content':
            # Помещаем сгенерированный SQL в содержимое FlowFile
            return FlowFileTransformResult(
                relationship='success',
                contents=sql_generated,
                attributes=flowfile.getAttributes()
            )
        elif output_destination == 'flowfile-attribute':
            # Добавляем сгенерированный SQL в указанный атрибут FlowFile
            attributes = {attribute_name: sql_generated}
            return FlowFileTransformResult(
                relationship='success',
                attributes=attributes
            )
        else:
            self.logger.error('Недопустимое значение Output Destination: {}'.format(output_destination))
            return FlowFileTransformResult(relationship='failure')

    def evaluate_expression(self, expression, json_data):
        # Парсим JSONPath выражение
        jsonpath_expr = parse(expression)
        matches = [match.value for match in jsonpath_expr.find(json_data)]
        if not matches:
            raise ValueError('Нет совпадений для выражения: {}'.format(expression))
        # Если несколько значений, объединяем их через запятую
        if len(matches) > 1:
            return ','.join(str(m) for m in matches)
        else:
            return str(matches[0])
