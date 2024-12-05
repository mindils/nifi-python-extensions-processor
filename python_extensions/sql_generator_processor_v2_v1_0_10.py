from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json
import re
import jmespath
from nifiapi.properties import PropertyDescriptor, StandardValidators

def get_parameters(data, expression):
    """
    Получение параметров из данных с помощью выражения jmespath.

    :param data: Данные (словарь или список).
    :param expression: Выражение jmespath для извлечения данных.
    :return: Извлечённые данные.
    """
    # Убираем $. или $ в начале выражения
    cleaned_expression = expression.lstrip("$").lstrip(".")
    return jmespath.search(cleaned_expression, data)

class ExpressionParser:
    def __init__(self, s):
        self.s = s
        self.index = 0
        self.length = len(s)

    def skip_whitespace(self):
        while self.index < self.length and self.s[self.index].isspace():
            self.index += 1

    def peek(self):
        if self.index < self.length:
            return self.s[self.index]
        else:
            return ''

    def consume(self):
        if self.index < self.length:
            ch = self.s[self.index]
            self.index += 1
            return ch
        else:
            return ''

    def parse_expression(self):
        self.skip_whitespace()
        ch = self.peek()
        if ch.isalpha():
            return self.parse_function_call_or_variable()
        elif ch == '$':
            return self.parse_variable()
        elif ch == "'" or ch == '"':
            return self.parse_string()
        elif ch.isdigit():
            return self.parse_number()
        else:
            raise ValueError(f"Unexpected character at position {self.index}: {ch}")

    def parse_function_call_or_variable(self):
        start = self.index
        while self.peek().isalnum() or self.peek() == '_':
            self.consume()
        name = self.s[start:self.index]
        self.skip_whitespace()
        if self.peek() == '(':
            return self.parse_function_call(name)
        else:
            return ('variable', name)

    def parse_function_call(self, name):
        self.consume()  # Consume '('
        args = []
        while True:
            self.skip_whitespace()
            if self.peek() == ')':
                self.consume()  # Consume ')'
                break
            arg = self.parse_expression()
            args.append(arg)
            self.skip_whitespace()
            if self.peek() == ',':
                self.consume()  # Consume ','
            elif self.peek() == ')':
                continue
            else:
                raise ValueError(f"Expected ',' or ')' at position {self.index}")
        return ('function', name, args)

    def parse_variable(self):
        start = self.index  # Position of '$'
        self.consume()  # Consume '$'
        while self.peek() and self.peek() not in [' ', ',', ')', '(', "'"]:
            self.consume()
        var = self.s[start:self.index]  # Include '$'
        return ('variable', var)

    def parse_string(self):
        quote = self.consume()  # Consume quote
        start = self.index
        while self.peek() != quote:
            if self.peek() == '':
                raise ValueError("Unterminated string")
            self.consume()
        s = self.s[start:self.index]
        self.consume()  # Consume closing quote
        return ('string', s)

    def parse_number(self):
        start = self.index
        while self.peek().isdigit():
            self.consume()
        num = self.s[start:self.index]
        return ('number', int(num))

def evaluate(expr, data):
    if expr[0] == 'function':
        func_name = expr[1]
        args = [evaluate(arg, data) for arg in expr[2]]
        return call_function(func_name, args)
    elif expr[0] == 'variable':
        varname = expr[1]
        return get_variable_value(varname, data)
    elif expr[0] == 'string':
        return expr[1]
    elif expr[0] == 'number':
        return expr[1]
    else:
        raise ValueError(f"Unknown expression type: {expr[0]}")

def get_variable_value(varname, data):
    expression = varname.lstrip('$').lstrip('.')
    return get_parameters(data, expression)

def call_function(func_name, args):
    if func_name == 'toInt':
        return toInt(*args)
    elif func_name == 'toStr':
        return toStr(*args)
    elif func_name == 'append':
        return append(*args)
    else:
        raise ValueError(f"Unknown function: {func_name}")

def toInt(*args):
    result = []
    for arg in args:
        if isinstance(arg, list):
            result.extend([int(a) for a in arg])
        else:
            result.append(int(arg))
    return result if len(result) > 1 else result[0]

def toStr(*args):
    result = []
    for arg in args:
        if isinstance(arg, list):
            result.extend([str(a) for a in arg])
        else:
            result.append(str(arg))
    return result if len(result) > 1 else result[0]

def append(*args):
    result = []
    suffix = args[-1] if isinstance(args[-1], str) else ''
    for arg in args[:-1]:
        if isinstance(arg, list):
            result.extend([f"'{a}'{suffix}" for a in arg])
        else:
            result.append(f"'{arg}'{suffix}")
    return ','.join(result)

def replace_placeholders(template, data):
    """
    Заменяет плейсхолдеры в шаблоне значениями из данных.

    :param template: Строка шаблона с плейсхолдерами.
    :param data: Данные для подстановки (словарь или список).
    :return: Строка с заменёнными плейсхолдерами.
    """

    def replacer(match):
        expression = match.group(1).strip()
        parser = ExpressionParser(expression)
        parsed_expr = parser.parse_expression()
        result = evaluate(parsed_expr, data)

        # Проверяем, является ли выражение функцией
        if isinstance(parsed_expr, tuple) and parsed_expr[0] == 'function':
            if isinstance(result, list):
                return ", ".join(str(item) for item in result)
            else:
                return str(result)
        else:
            if isinstance(result, list):
                return ", ".join(f"'{item}'" if isinstance(item, str) else str(item) for item in result)
            else:
                return f"'{result}'" if isinstance(result, str) else str(result)

    pattern = r"\${\s*(.*?)\s*}"
    return re.sub(pattern, replacer, template)

class SQLGeneratorProcessorV2(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.10'
        description = 'Генерирует SQL-запросы, заменяя плейсхолдеры значениями из JSON с использованием выражений.'
        tags = ['SQL', 'JSON', 'Template']
        dependencies = ['jmespath']

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        sql_template = PropertyDescriptor(
            name='SQL Template',
            description='SQL-шаблон с плейсхолдерами, которые будут заменены значениями из JSON. Используйте выражения внутри ${...}.',
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
            description='SQL-шаблон, используемый, если Conditional Key имеет Conditional Value. Используйте выражения внутри ${...}.',
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
                sql = replace_placeholders(sql_template, data_non_matching)
                sql_statements.append(sql)
            except Exception as e:
                self.logger.error('Ошибка при генерации SQL для несовпадающих данных: {}'.format(e))
                return self.handle_error(flowfile, error_handling)

        # Для data_matching используем conditional_sql_template
        if data_matching and conditional_sql_template:
            try:
                sql = replace_placeholders(conditional_sql_template, data_matching)
                sql_statements.append(sql)
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

        key = key_path.split('.')[-1]
        if isinstance(data, list):
            data_matching = [item for item in data if item.get(key) == value]
            data_non_matching = [item for item in data if item.get(key) != value]
        elif isinstance(data, dict):
            if data.get(key) == value:
                data_matching = [data]
                data_non_matching = []
            else:
                data_matching = []
                data_non_matching = [data]
        else:
            raise ValueError("Неподдерживаемый тип данных. Ожидался список или словарь.")

        return data_matching, data_non_matching

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
