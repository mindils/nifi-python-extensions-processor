from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json
import re
from jsonpath_ng import parse
from nifiapi.properties import PropertyDescriptor

class SQLGeneratorProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.4'
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

        store_in_content = PropertyDescriptor(
            name='Store in Content',
            description='Указывает, помещать ли SQL в содержимое FlowFile (true) или в атрибут (false).',
            required=False,
            default_value='false',
        )

        attribute_name = PropertyDescriptor(
            name='Attribute Name',
            description='Имя атрибута, в который будет помещен сгенерированный SQL, если выбрано размещение в атрибуте.',
            required=False,
            default_value='generated_sql',
        )

        return [sql_template, store_in_content, attribute_name]

    def transform(self, context, flowfile):
        # Получаем шаблон SQL из свойств процессора
        sql_template_prop = context.getProperty('SQL Template')
        sql_template = sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()
        if not sql_template:
            self.logger.error('Свойство SQL Template не установлено.')
            return FlowFileTransformResult(relationship='failure')

        # Получаем настройку Store in Content
        store_in_content_prop = context.getProperty('Store in Content')
        store_in_content = store_in_content_prop.evaluateAttributeExpressions(flowfile).getValue().lower() == 'true'

        # Если выбран атрибут, получаем имя атрибута
        attribute_name_prop = context.getProperty('Attribute Name')
        attribute_name = attribute_name_prop.evaluateAttributeExpressions(flowfile).getValue() or 'generated_sql'

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

        if store_in_content:
            # Помещаем сгенерированный SQL в содержимое FlowFile
            return FlowFileTransformResult(
                relationship='success',
                contents=sql_generated,
                attributes=flowfile.getAttributes()
            )
        else:
            # Добавляем сгенерированный SQL в указанный атрибут FlowFile
            attributes = {attribute_name: sql_generated}
            return FlowFileTransformResult(
                relationship='success',
                attributes=attributes
            )

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
