from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

class ConvertRecordKeysToCase(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '1.1.4'
        description = 'Приводит ключи записей JSON и Avro к нижнему или верхнему регистру с использованием итеративного обхода структуры данных. Оптимизирован для сложных и вложенных данных.'
        tags = ['json', 'avro', 'case', 'keys', 'optimization', 'record', 'lowercase', 'uppercase']

    def __init__(self, **kwargs):
        super().__init__()  # Call the base class constructor
        self.convert_case = PropertyDescriptor(
            name="Convert to Lower Case",
            description="Specify 'true' to convert keys to lower case, 'false' to convert to upper case.",
            default_value="true",
            required=True,
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )
        self.descriptors = [self.convert_case]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, record, schema, attributemap):
        # Get the property value
        convert_to_lower_case = context.getProperty("Convert to Lower Case").getValue()
        to_lower = convert_to_lower_case.lower() == 'true'

        # Optimize key transformation
        new_record = self.convert_keys_case(record, to_lower)

        return RecordTransformResult(
            record=new_record
        )

    def convert_keys_case(self, data, to_lower):
        """
        Optimized function to convert keys of a record to lower or upper case.
        """
        stack = [(data, None, None)]  # Stack for iterative traversal
        result = None

        while stack:
            current, parent, parent_key = stack.pop()

            if isinstance(current, dict):
                new_dict = {}
                for key, value in current.items():
                    # Optimize key conversion
                    new_key = key.lower() if to_lower else key.upper()
                    new_dict[new_key] = value
                    stack.append((value, new_dict, new_key))
                if parent is None:
                    result = new_dict
                else:
                    parent[parent_key] = new_dict
            elif isinstance(current, list):
                new_list = []
                for item in current:
                    new_list.append(item)
                    stack.append((item, new_list, len(new_list) - 1))
                if parent is None:
                    result = new_list
                else:
                    parent[parent_key] = new_list
            else:
                if parent is not None:
                    parent[parent_key] = current

        return result
