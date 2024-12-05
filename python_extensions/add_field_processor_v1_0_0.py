from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import datetime
import re

class AddFieldFromTemplate(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'Adds a new field to each record based on a template.'
        tags = ['record', 'add field', 'template', 'date formatting']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.FIELD_NAME = PropertyDescriptor(
            name='Field Name',
            description='The name of the field to add to the output record.',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.TEMPLATE = PropertyDescriptor(
            name='Template',
            description='A template for generating the value of the new field.',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.DEFAULT_DATE_FORMAT = PropertyDescriptor(
            name='Default Date Format',
            description='Default date format to use if not specified in the template.',
            required=False,
            default_value='YYYY-MM-DD H24:MM:SS',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.descriptors = [self.FIELD_NAME, self.TEMPLATE, self.DEFAULT_DATE_FORMAT]

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        from nifiapi.relationship import Relationship
        return [Relationship(name='success', description='All records are routed to success')]

    def transform(self, context, record, schema, attributemap):
        # Get the property values
        field_name = context.getProperty('Field Name').evaluateAttributeExpressions(attributemap).getValue()
        template = context.getProperty('Template').evaluateAttributeExpressions(attributemap).getValue()
        default_date_format = context.getProperty('Default Date Format').evaluateAttributeExpressions(attributemap).getValue()

        # Process the template
        def process_template(template, record, default_date_format):
            # Regex to find variables like ${field} or ${field:date("format")}
            pattern = r'\$\{\s*\$?\.?([^\}:]+?)(?::([^\(]+?)(?:\("([^"]+)"\))?)?\s*\}'

            def replacer(match):
                field_name = match.group(1).strip()
                operation = match.group(2)
                format_spec = match.group(3)

                value = record.get(field_name)

                if value is None:
                    return ''

                if operation == 'date':
                    # Handle date formatting
                    if isinstance(value, str):
                        # Try to parse the date string
                        try:
                            # Try to parse the date string in ISO format
                            dt = datetime.datetime.fromisoformat(value)
                        except ValueError:
                            # Try parsing common date formats
                            try:
                                dt = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                try:
                                    dt = datetime.datetime.strptime(value, '%Y-%m-%d')
                                except ValueError:
                                    # If parsing fails, return the value as is
                                    return value
                    elif isinstance(value, datetime.datetime):
                        dt = value
                    else:
                        return str(value)

                    if format_spec:
                        date_format = format_spec
                    else:
                        date_format = default_date_format

                    # Convert Java date format to Python date format
                    date_format_python = self.convert_java_date_format_to_python(date_format)

                    formatted_date = dt.strftime(date_format_python)
                    return formatted_date
                else:
                    # No operation, just convert to string
                    return str(value)

            result = re.sub(pattern, replacer, template)
            return result

        # Function to convert Java date format to Python date format
        def convert_java_date_format_to_python(java_format):
            # Map Java date format symbols to Python's
            mapping = {
                'YYYY': '%Y',
                'YY': '%y',
                'MM': '%m',
                'dd': '%d',
                'DD': '%d',
                'HH': '%H',
                'hh': '%I',
                'mm': '%M',
                'ss': '%S',
                'SS': '%f',  # For microseconds, adjust as needed
                'H24': '%H',  # Assuming H24 maps to 24-hour clock
                'H12': '%I',
            }

            # Replace Java format symbols with Python's
            for java_symbol, python_symbol in mapping.items():
                java_format = java_format.replace(java_symbol, python_symbol)

            return java_format

        # Process the template to generate the new field value
        new_field_value = process_template(template, record, default_date_format)

        # Add the new field to the record
        record[field_name] = new_field_value

        # Return the result
        return RecordTransformResult(record=record)

