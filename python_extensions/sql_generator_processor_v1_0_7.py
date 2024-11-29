from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json
import re
from nifiapi.properties import PropertyDescriptor, StandardValidators

class SQLGeneratorProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.7'
        description = 'Генерирует SQL-запросы, заменяя плейсхолдеры значениями из JSON с использованием JSONPath. Поддерживает условный SQL на основе совпадения ключ-значение в JSON.'
        tags = ['SQL', 'JSON', 'Template']
        # Убираем зависимость от jsonpath-ng
        # dependencies = ['jsonpath-ng']

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        sql_template = PropertyDescriptor(
            name='SQL Template',
            description='SQL-шаблон с плейсхолдерами, которые будут заменены значениями из JSON. Используйте JSONPath выражения внутри ${...}.',
            required=True,
            default_value='',
        )

        store_in_content = PropertyDescriptor(
            name='Store in Content',
            description='Указывает, помещать ли SQL в контент FlowFile (true) или в атрибут (false).',
            required=False,
            default_value='false',
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )

        attribute_name = PropertyDescriptor(
            name='Attribute Name',
            description='Имя атрибута, в который будет помещен сгенерированный SQL, если сохраняется в атрибуте.',
            required=False,
            default_value='generated_sql',
        )

        conditional_key = PropertyDescriptor(
            name='Conditional Key',
            description='Имя ключа в JSON для проверки на определенное значение.',
            required=False,
            default_value='',
        )

        conditional_value = PropertyDescriptor(
            name='Conditional Value',
            description='Значение, которое должен иметь Conditional Key для использования Conditional SQL Template.',
            required=False,
            default_value='',
        )

        conditional_sql_template = PropertyDescriptor(
            name='Conditional SQL Template',
            description='SQL-шаблон для использования, если Conditional Key имеет Conditional Value. Используйте JSONPath выражения внутри ${...}.',
            required=False,
            default_value='',
        )

        error_handling = PropertyDescriptor(
            name='Error Handling',
            description=('Указывает, как обрабатывать ошибки:\n'
                         '1. "pass": Записать ошибку в лог и передать FlowFile дальше.\n'
                         '2. "original": Вернуть FlowFile в исходную очередь (по умолчанию).\n'
                         '3. "failure": Отправить FlowFile в связь failure.\n'
                         'Если указано неизвестное значение, используется "original".'),
            required=False,
            default_value='original',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        return [
            sql_template,
            store_in_content,
            attribute_name,
            conditional_key,
            conditional_value,
            conditional_sql_template,
            error_handling
        ]

    def transform(self, context, flowfile):
        # Получаем свойства
        sql_template_prop = context.getProperty('SQL Template')
        sql_template = sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()

        store_in_content_prop = context.getProperty('Store in Content')
        store_in_content = store_in_content_prop.evaluateAttributeExpressions(flowfile).getValue().lower() == 'true'

        attribute_name_prop = context.getProperty('Attribute Name')
        attribute_name = attribute_name_prop.evaluateAttributeExpressions(flowfile).getValue() or 'generated_sql'

        conditional_key_prop = context.getProperty('Conditional Key')
        conditional_key = conditional_key_prop.evaluateAttributeExpressions(flowfile).getValue()

        conditional_value_prop = context.getProperty('Conditional Value')
        conditional_value = conditional_value_prop.evaluateAttributeExpressions(flowfile).getValue()

        conditional_sql_template_prop = context.getProperty('Conditional SQL Template')
        conditional_sql_template = conditional_sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()

        error_handling_prop = context.getProperty('Error Handling')
        error_handling = (error_handling_prop.evaluateAttributeExpressions(flowfile).getValue() or 'original').lower()

        # Читаем контент FlowFile и парсим JSON
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')
        try:
            json_data = json.loads(content_str)
        except Exception as e:
            self.logger.error('Ошибка обработки: {}'.format(e))
            return self.handle_error(flowfile, error_handling)

        # Определяем, какой SQL-шаблон использовать
        use_conditional_sql = False
        if conditional_key and conditional_value and conditional_sql_template:
            try:
                # Оцениваем значение условного ключа
                actual_value = self.evaluate_expression(conditional_key, json_data)
                if actual_value == conditional_value:
                    use_conditional_sql = True
                    sql_template_to_use = conditional_sql_template
                else:
                    sql_template_to_use = sql_template
            except Exception as e:
                self.logger.error('Ошибка при оценке Conditional Key: {}'.format(e))
                return self.handle_error(flowfile, error_handling)
        else:
            sql_template_to_use = sql_template

        if not sql_template_to_use:
            self.logger.error('SQL Template не установлен.')
            return self.handle_error(flowfile, error_handling)

        # Функция для замены плейсхолдеров
        def replace_placeholder(match):
            expression = match.group(1)
            try:
                value = self.evaluate_expression(expression, json_data)
                return value
            except Exception as e:
                self.logger.error('Ошибка при обработке выражения {}: {}'.format(expression, e))
                raise  # Запускаем обработку ошибок

        try:
            # Заменяем плейсхолдеры в выбранном SQL-шаблоне
            sql_generated = re.sub(r'\$\{([^}]+)\}', replace_placeholder, sql_template_to_use)
        except Exception as e:
            return self.handle_error(flowfile, error_handling)

        if store_in_content:
            # Помещаем сгенерированный SQL в контент FlowFile
            return FlowFileTransformResult(
                relationship='success',
                contents=sql_generated,
                attributes=flowfile.getAttributes()
            )
        else:
            # Создаем новый словарь атрибутов
            attributes = dict(flowfile.getAttributes())  # Копируем существующие атрибуты
            attributes[attribute_name] = sql_generated  # Добавляем сгенерированный SQL
            return FlowFileTransformResult(
                relationship='success',
                attributes=attributes
            )

    def handle_error(self, flowfile, error_handling):
        """
        Обрабатываем ошибки в соответствии со стратегией обработки ошибок.
        """
        if error_handling == 'pass':
            # Записываем ошибку в лог, но передаем FlowFile дальше
            self.logger.warn("Произошла ошибка, передаем FlowFile в связь success.")
            return FlowFileTransformResult(relationship='success')
        elif error_handling == 'failure':
            # Отправляем в связь failure
            return FlowFileTransformResult(relationship='failure')
        else:
            # По умолчанию: возвращаем FlowFile в исходную очередь, выбрасывая исключение
            self.logger.warn("Произошла ошибка, возвращаем FlowFile в исходную очередь.")
            raise Exception("Произошла ошибка, возвращаем FlowFile в исходную очередь.")

    def evaluate_expression(self, expression, json_data):
        # Собственная реализация для простых выражений JSONPath
        # Шаблон 1: $.variable_name
        m = re.match(r'^\$\.(\w+)$', expression)
        if m:
            key = m.group(1)
            if key in json_data:
                return str(json_data[key])
            else:
                raise ValueError(f"Ключ '{key}' не найден в JSON данных.")
        # Шаблон 2: $[index].name_key
        m = re.match(r'^\$\[(\d+)\]\.(\w+)$', expression)
        if m:
            index = int(m.group(1))
            key = m.group(2)
            if isinstance(json_data, list) and index < len(json_data):
                item = json_data[index]
                if key in item:
                    return str(item[key])
                else:
                    raise ValueError(f"Ключ '{key}' не найден в элементе с индексом {index}.")
            else:
                raise ValueError(f"Индекс {index} вне диапазона.")
        # Шаблон 3: $[*].key
        m = re.match(r'^\$\[\*\]\.(\w+)$', expression)
        if m:
            key = m.group(1)
            if isinstance(json_data, list):
                values = []
                for item in json_data:
                    if key in item:
                        values.append(str(item[key]))
                    else:
                        raise ValueError(f"Ключ '{key}' не найден в одном из элементов списка.")
                return ','.join(values)
            else:
                raise ValueError("JSON данные не являются списком.")
        else:
            raise ValueError(f"Неподдерживаемое выражение: {expression}")
