from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json

class RemoveDuplicatesByKey(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.0'
        description = 'Removes duplicates from a JSON array based on specified key(s), keeping the last occurrence.'
        tags = ['json', 'duplicate', 'array']

    def __init__(self, **kwargs):
        self.keys_property = PropertyDescriptor(
            name="Keys",
            description="Comma-separated list of keys to determine duplicates in JSON array.",
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.descriptors = [self.keys_property]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        # Get the keys from the property
        keys_str = context.getProperty("Keys").getValue()
        keys = [key.strip() for key in keys_str.split(',') if key.strip()]

        # Get the contents of the FlowFile
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')

        try:
            data = json.loads(content_str)
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON input: {e}")
            # Route to failure relationship
            return FlowFileTransformResult(relationship='failure')

        if isinstance(data, list):
            # Process the array
            seen_keys = set()
            result_list = []
            # Process the array in reverse to keep the last occurrence
            for item in reversed(data):
                # Build the key based on the specified keys
                key_values = tuple(str(item.get(k, None)) for k in keys)
                if key_values not in seen_keys:
                    seen_keys.add(key_values)
                    # Insert at the beginning to maintain order
                    result_list.insert(0, item)
            new_content_str = json.dumps(result_list)
        else:
            # If data is not a list, output it unchanged
            new_content_str = content_str

        # Return the new content
        return FlowFileTransformResult(
            relationship='success',
            contents=new_content_str
        )
