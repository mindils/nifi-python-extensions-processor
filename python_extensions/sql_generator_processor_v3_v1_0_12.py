from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import re
from jsonpath_ng import parse

class SQLGeneratorProcessorV3(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.12'
        description = 'Генерирует SQL-запросы, заменяя плейсхолдеры значениями из JSON с использованием JSONPath выражений.'
        tags = ['SQL', 'JSON', 'Template']
        dependencies = ['jsonpath-ng']

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = self.getPropertyDescriptors()

    def getPropertyDescriptors(self):
        sql_template = PropertyDescriptor(
            name='SQL Template',
            description='SQL-шаблон с плейсхолдерами, которые будут заменены значениями из JSON. Используйте JSONPath выражения внутри ${...}.',
            required=True,
            default_value='',
        )

        conditional_key = PropertyDescriptor(
            name='Conditional Key',
            description='JSONPath-выражение для идентификации ключа в JSON для условного разделения.',
            required=False,
            default_value='',
        )

        conditional_value = PropertyDescriptor(
            name='Conditional Value',
            description='Значение, которое должен иметь Conditional Key, чтобы использовать Conditional SQL Template.',
            required=False,
            default_value='',
        )

        conditional_sql_template = PropertyDescriptor(
            name='Conditional SQL Template',
            description='SQL-шаблон, используемый, если Conditional Key имеет Conditional Value. Используйте JSONPath выражения внутри ${...}.',
            required=False,
            default_value='',
        )

        error_handling = PropertyDescriptor(
            name='Error Handling',
            description=('Указывает, как обрабатывать ошибки:\n'
                         '1. "pass": Записать ошибку в журнал и передать FlowFile дальше.\n'
                         '2. "original": Вернуть FlowFile в исходную очередь (по умолчанию).\n'
                         '3. "failure": Отправить FlowFile в связь "failure".\n'
                         'Если указано неизвестное значение, используется "original".'),
            required=False,
            default_value='original',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        return [
            sql_template,
            conditional_key,
            conditional_value,
            conditional_sql_template,
            error_handling
        ]

    def transform(self, context, flowfile):
        # Получение свойств
        sql_template_prop = context.getProperty('SQL Template')
        sql_template = sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()

        conditional_sql_template_prop = context.getProperty('Conditional SQL Template')
        conditional_sql_template = conditional_sql_template_prop.evaluateAttributeExpressions(flowfile).getValue()

        conditional_key_prop = context.getProperty('Conditional Key')
        conditional_key = conditional_key_prop.evaluateAttributeExpressions(flowfile).getValue()

        conditional_value_prop = context.getProperty('Conditional Value')
        conditional_value = conditional_value_prop.evaluateAttributeExpressions(flowfile).getValue()

        error_handling_prop = context.getProperty('Error Handling')
        error_handling = (error_handling_prop.evaluateAttributeExpressions(flowfile).getValue() or 'original').lower()

        # Чтение содержимого FlowFile
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')
        try:
            json_data = json.loads(content_str)
        except Exception as e:
            self.logger.error('Ошибка при разборе JSON данных: {}'.format(e))
            return self.handle_error(flowfile, error_handling)

        # Убедимся, что json_data является списком
        if isinstance(json_data, dict):
            json_data = [json_data]
        elif not isinstance(json_data, list):
            self.logger.error('Неверный формат JSON данных. Ожидался список или словарь.')
            return self.handle_error(flowfile, error_handling)

        # Разделение данных на основе Conditional Key и Value
        data_matching, data_non_matching = self.split_json(json_data, conditional_key, conditional_value)

        # Генерация SQL запросов
        sql_statements = []

        # Для data_non_matching используем sql_template
        if data_non_matching and sql_template:
            try:
                sql = self.generate_sql(data_non_matching, sql_template)
                sql_statements.extend(sql)
            except Exception as e:
                self.logger.error('Ошибка при генерации SQL для несовпадающих данных: {}'.format(e))
                return self.handle_error(flowfile, error_handling)

        # Для data_matching используем conditional_sql_template
        if data_matching and conditional_sql_template:
            try:
                sql = self.generate_sql(data_matching, conditional_sql_template)
                sql_statements.extend(sql)
            except Exception as e:
                self.logger.error('Ошибка при генерации SQL для совпадающих данных: {}'.format(e))
                return self.handle_error(flowfile, error_handling)

        # Объединяем SQL запросы
        output_sql = '; '.join(sql_statements)

        # Возвращаем результат
        return FlowFileTransformResult(
            relationship='success',
            contents=output_sql,
            attributes=flowfile.getAttributes()
        )

    def split_json(self, data, key_path, value):
        """
        Разделяет JSON на два списка по указанному условию.

        :param data: исходный JSON (список или словарь)
        :param key_path: путь до ключа в формате JSONPath, например '$[*].type' или '$.type'
        :param value: значение, по которому нужно разделить
        :return: два JSON объекта
        """
        if not key_path:
            # Если Conditional Key не указан, все данные считаются несовпадающими
            return [], data

        try:
            jsonpath_expr = parse(key_path)
            if isinstance(data, list):
                data_matching = [item for item in data if any(match.value == value for match in jsonpath_expr.find(item))]
                data_non_matching = [item for item in data if not any(match.value == value for match in jsonpath_expr.find(item))]
            elif isinstance(data, dict):
                if any(match.value == value for match in jsonpath_expr.find(data)):
                    data_matching = [data]
                    data_non_matching = []
                else:
                    data_matching = []
                    data_non_matching = [data]
            else:
                raise ValueError("Неподдерживаемый тип данных. Ожидался список или словарь.")
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге JSONPath: {e}")
            raise

        return data_matching, data_non_matching

    def generate_sql(self, data, template):
        """
        Генерирует SQL-запросы на основе данных и шаблона.

        :param data: Данные для подстановки (список или словарь).
        :param template: Шаблон SQL-запроса с плейсхолдерами.
        :return: Список SQL-запросов.
        """
        jsonpaths = self.extract_jsonpaths(template)
        requires_aggregation = any('[*]' in jp for jp in jsonpaths)

        if isinstance(data, list) and requires_aggregation:
            replacements = {}
            for jp in jsonpaths:
                values = self.evaluate_jsonpath(data, jp)
                str_values = [f"'{v}'" if isinstance(v, (str, bytes)) else str(v) for v in values]
                replacements[jp] = ','.join(str_values)
            sql = template
            for jp in jsonpaths:
                placeholder = f'${{{jp}}}'
                sql = sql.replace(placeholder, replacements[jp])
            return [sql + ';']

        if isinstance(data, list):
            sqls = []
            for item in data:
                replacements = {}
                for jp in jsonpaths:
                    values = self.evaluate_jsonpath(item, jp)
                    value = str(values[0]) if values else ''
                    replacements[jp] = value
                sql = template
                for jp in jsonpaths:
                    placeholder = f'${{{jp}}}'
                    sql = sql.replace(placeholder, replacements[jp])
                sqls.append(sql + ';')
            return sqls

        replacements = {}
        for jp in jsonpaths:
            values = self.evaluate_jsonpath(data, jp)
            value = str(values[0]) if values else ''
            replacements[jp] = value
        sql = template
        for jp in jsonpaths:
            placeholder = f'${{{jp}}}'
            sql = sql.replace(placeholder, replacements[jp])
        return [sql + ';']

    def extract_jsonpaths(self, template):
        """
        Извлекает JSONPath выражения из шаблона.

        :param template: Шаблон SQL-запроса.
        :return: Список JSONPath выражений.
        """
        pattern = r'\$\{(.*?)\}'
        return re.findall(pattern, template)

    def evaluate_jsonpath(self, data, jsonpath_expr):
        """
        Вычисляет значение JSONPath выражения.

        :param data: Данные для подстановки.
        :param jsonpath_expr: JSONPath выражение.
        :return: Список значений.
        """
        try:
            jsonpath = parse(jsonpath_expr)
            matches = jsonpath.find(data)
            return [match.value for match in matches]
        except Exception as e:
            self.logger.error(f"Ошибка при вычислении JSONPath: {jsonpath_expr}, {e}")
            return []

    def handle_error(self, flowfile, error_handling):
        """
        Обрабатывает ошибки в соответствии с заданной стратегией.

        :param flowfile: Обрабатываемый FlowFile.
        :param error_handling: Стратегия обработки ошибок.
        :return: FlowFileTransformResult или вызывает исключение.
        """
        if error_handling == 'pass':
            # Записать ошибку в журнал, но передать FlowFile дальше
            self.logger.warning("Произошла ошибка, передаем FlowFile в связь success.")
            return FlowFileTransformResult(relationship='success', contents=flowfile.getContentsAsBytes(), attributes=flowfile.getAttributes())
        elif error_handling == 'failure':
            # Отправить в связь failure
            return FlowFileTransformResult(relationship='failure')
        else:
            # По умолчанию: вернуть FlowFile в исходную очередь, вызвав исключение
            self.logger.warning("Произошла ошибка, возвращаем FlowFile в исходную очередь.")
            raise Exception("An error occurred, returning the FlowFile to the original queue.")