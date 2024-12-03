from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import re

class ConvertJsonKeysToCase(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.1'
        description = 'Приводит ключи JSON к нижнему или верхнему регистру с использованием регулярных выражений. Поддерживает только текстовые JSON данные.'
        tags = ['json', 'case', 'keys', 'regex', 'text', 'lowercase', 'uppercase']

    def __init__(self, **kwargs):
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

    def transform(self, context, flowfile):
        # Get the property value
        convert_to_lower_case = context.getProperty("Convert to Lower Case").getValue()
        if convert_to_lower_case.lower() == 'true':
            to_lower = True
        else:
            to_lower = False

        # Get the contents of the FlowFile as bytes, then decode to string
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')

        # Perform the regex replacement to change the keys
        def replace_key(match):
            key = match.group(1)
            if to_lower:
                new_key = key.lower()
            else:
                new_key = key.upper()
            return f'"{new_key}":'

        # Perform the substitution
        new_content_str = re.sub(r'"([^"]+)":', replace_key, content_str)

        # Return the new content
        return FlowFileTransformResult(
            relationship='success',
            contents=new_content_str
        )
