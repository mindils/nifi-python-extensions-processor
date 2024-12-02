from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

class ConvertRecordKeysCase(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '1.1.1'
        description = 'Converts record keys to lower or upper case.'
        tags = ['json', 'avro', 'case', 'keys']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)  # Call the base class constructor
        self.convert_case = PropertyDescriptor(
            name="Convert to Lower Case",
            description="Specify \'true\' to convert keys to lower case, \'false\' to convert to upper case.",
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

        # Transform the keys in the record
        new_record = self.convert_keys_case(record, to_lower)

        return RecordTransformResult(
            record=new_record
        )

    def convert_keys_case(self, data, to_lower):
        if isinstance(data, dict):
            new_data = {}
            for key, value in data.items():
                if to_lower:
                    new_key = key.lower()
                else:
                    new_key = key.upper()
                new_data[new_key] = self.convert_keys_case(value, to_lower)
            return new_data
        elif isinstance(data, list):
            return [self.convert_keys_case(item, to_lower) for item in data]
        else:
            return data
