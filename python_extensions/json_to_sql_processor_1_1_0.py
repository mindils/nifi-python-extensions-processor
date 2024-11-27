from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ControllerServiceDefinition
from nifiapi.relationship import Relationship
import json

class JsonToSqlFlowFileProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.1.0'
        description = 'Преобразует JSON из содержимого FlowFile в SQL команды для PostgreSQL.'
        tags = ['JSON', 'SQL', 'PostgreSQL', 'Database', 'Upsert']

    def __init__(self, **kwargs):

        # 1. Поле в JSON, содержащее тип операции (I, U, D)
        self.operation_field = PropertyDescriptor(
            name="Operation Field",
            description="Поле в JSON, из которого будет браться тип операции (I - вставка, U - обновление, D - удаление).",
            required=True,
            default_value="operation",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        # 2. Название таблицы или таблиц (через запятую)
        self.table_names = PropertyDescriptor(
            name="Table Names",
            description="Название таблицы или таблиц для выполнения операций (можно указать несколько через запятую).",
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        # 3. Ключевые поля для обновления или удаления (через запятую)
        self.key_fields = PropertyDescriptor(
            name="Key Fields",
            description="Ключевые поля для операций обновления или удаления (указать через запятую).",
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        # 4. Использовать UPSERT для операций вставки и обновления
        self.use_upsert = PropertyDescriptor(
            name="Use Upsert",
            description="Если true, операции вставки и обновления будут сгенерированы как UPSERT.",
            required=False,
            default_value="false",
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )

        # 5. Database Connection Pooling Service
        self.db_connection_service = PropertyDescriptor(
            name="Database Connection Pooling Service",
            description="Ссылка на Database Connection Pooling Service для выполнения запросов к базе данных.",
            required=True,
            identifies_controller_service=ControllerServiceDefinition('org.apache.nifi.dbcp.DBCPService')
        )

        self.descriptors = [
            self.operation_field,
            self.table_names,
            self.key_fields,
            self.use_upsert,
            self.db_connection_service,
        ]

        # Определение отношений
        self.relationship_success = Relationship('success', 'Успешные SQL команды.')
        self.relationship_failure = Relationship('failure', 'Неуспешные преобразования.')

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        return [self.relationship_success, self.relationship_failure]

    def onScheduled(self, context):
        # Получаем Controller Service для подключения к базе данных
        self.connection_service = context.getProperty(self.db_connection_service.name).asControllerService()
        if not self.connection_service:
            raise ValueError("Не настроен Database Connection Pooling Service!")

    def transform(self, context, flowfile):
        try:
            # Получение настроек из свойств процессора
            operation_field = context.getProperty(self.operation_field.name).getValue()
            table_names = [table.strip() for table in context.getProperty(self.table_names.name).getValue().split(',')]
            key_fields = [key.strip() for key in context.getProperty(self.key_fields.name).getValue().split(',')]
            use_upsert = context.getProperty(self.use_upsert.name).asBoolean()

            # Получение списка колонок из базы данных
            columns_by_table = {}
            for table_name in table_names:
                columns_by_table[table_name] = self.get_table_columns(table_name)

            # Получение содержимого FlowFile и парсинг JSON
            content_bytes = flowfile.getContentsAsBytes()
            content_str = content_bytes.decode('utf-8')
            records = json.loads(content_str)

            if not isinstance(records, list):
                records = [records]

            # Генерация SQL для каждого объекта в массиве
            sql_statements = []
            for record in records:
                operation = record.get(operation_field)
                if operation == 'I':
                    sql = self.generate_insert_sql(record, table_names, use_upsert, key_fields, columns_by_table)
                elif operation == 'U':
                    sql = self.generate_update_sql(record, table_names, use_upsert, key_fields, columns_by_table)
                elif operation == 'D':
                    sql = self.generate_delete_sql(record, table_names, key_fields)
                else:
                    raise ValueError(f"Неизвестная операция: {operation}")
                sql_statements.append(sql)

            final_sql = ';\n'.join(sql_statements) + ';'

            # Создание нового словаря атрибутов
            attributes = dict(flowfile.getAttributes())
            attributes['sql'] = final_sql

            return FlowFileTransformResult(
                relationship=self.relationship_success.name,
                contents=final_sql,
                attributes=attributes
            )

        except Exception as e:
            self.logger.error(f"Ошибка при преобразовании FlowFile: {str(e)}")
            return FlowFileTransformResult(
                relationship=self.relationship_failure.name
            )

    def get_table_columns(self, table_name):
        """
        Получение списка колонок из таблицы базы данных.
        """
        try:
            # Получаем подключение из Controller Service
            connection = self.connection_service.getConnection()
            cursor = connection.cursor()
            # Выполняем запрос для получения имен колонок
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            connection.close()
            return columns
        except Exception as e:
            self.logger.error(f"Ошибка при получении колонок для таблицы {table_name}: {str(e)}")
            raise ValueError(f"Не удалось получить список колонок для таблицы {table_name}")

    def generate_insert_sql(self, record, table_names, use_upsert, key_fields, columns_by_table):
        sql_statements = []
        for table in table_names:
            columns = columns_by_table.get(table, record.keys())
            columns_str = ', '.join(columns)
            values = ', '.join([self.format_value(record.get(col)) for col in columns])
            if use_upsert:
                conflict_targets = ', '.join(key_fields)
                update_assignments = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in key_fields])
                sql = f"INSERT INTO {table} ({columns_str}) VALUES ({values}) ON CONFLICT ({conflict_targets}) DO UPDATE SET {update_assignments};"
            else:
                sql = f"INSERT INTO {table} ({columns_str}) VALUES ({values});"
            sql_statements.append(sql)
        return ' '.join(sql_statements)

    def generate_update_sql(self, record, table_names, use_upsert, key_fields, columns_by_table):
        if use_upsert:
            return self.generate_insert_sql(record, table_names, use_upsert=True, key_fields=key_fields, columns_by_table=columns_by_table)
        else:
            sql_statements = []
            for table in table_names:
                columns = columns_by_table.get(table, record.keys())
                set_assignments = ', '.join([f"{col}={self.format_value(record.get(col))}" for col in columns if col not in key_fields])
                where_conditions = ' AND '.join([f"{key}={self.format_value(record.get(key))}" for key in key_fields])
                sql = f"UPDATE {table} SET {set_assignments} WHERE {where_conditions};"
                sql_statements.append(sql)
            return ' '.join(sql_statements)

    def generate_delete_sql(self, record, table_names, key_fields):
        sql_statements = []
        for table in table_names:
            where_conditions = ' AND '.join([f"{key}={self.format_value(record.get(key))}" for key in key_fields])
            sql = f"DELETE FROM {table} WHERE {where_conditions};"
            sql_statements.append(sql)
        return ' '.join(sql_statements)

    def format_value(self, value):
        if isinstance(value, str):
            value = value.replace("'", "''")
            return f"'{value}'"
        elif value is None:
            return 'NULL'
        else:
            return str(value)
