from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import re
import hashlib
import uuid
from datetime import datetime


class AddUUIDField(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']

    class ProcessorDetails:
        version = '1.0.4'
        description = 'Adds a UUID field generated from a template to records, supports arrays of records, integer timestamps, and outputs raw string for debugging'

    def __init__(self, **kwargs):
        super().__init__()  # Call the base class initializer

    def getPropertyDescriptors(self):
        field_name = PropertyDescriptor(
            name='Field Name',
            description='Name of the field to add the UUID to',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        debug_field_name = PropertyDescriptor(
            name='Debug Field Name',
            description='Name of the field to store the raw concatenated string (for debugging)',
            required=False,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        uuid_template = PropertyDescriptor(
            name='UUID Template',
            description='Template for generating the UUID',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        input_date_format = PropertyDescriptor(
            name='Input Date Format',
            description='The format of input date fields in the record (used if the date is a string)',
            default_value='YYYY-MM-DD HH:mm:ss',
            required=False
        )

        output_date_format = PropertyDescriptor(
            name='Output Date Format',
            description='The format for outputting dates in the template',
            default_value='YYYY-MM-DD',
            required=False
        )

        timestamp_units = PropertyDescriptor(
            name='Timestamp Units',
            description='Units of Unix timestamps (if input date is an integer). Options: seconds, milliseconds',
            default_value='milliseconds',
            required=False,
            allowable_values=['seconds', 'milliseconds']
        )

        self.descriptors = [
            field_name,
            debug_field_name,
            uuid_template,
            input_date_format,
            output_date_format,
            timestamp_units,
        ]
        return self.descriptors

    def transform(self, context, record, schema, attributemap):
        # Get property values
        field_name = context.getProperty('Field Name').getValue()
        debug_field_name = context.getProperty('Debug Field Name').getValue()
        uuid_template = context.getProperty('UUID Template').getValue()
        input_date_format = context.getProperty('Input Date Format').getValue()
        output_date_format = context.getProperty('Output Date Format').getValue()
        timestamp_units = context.getProperty('Timestamp Units').getValue()

        # Function to convert date format
        def convert_date_format(format_str):
            mapping = {
                'YYYY': '%Y',
                'MM': '%m',
                'DD': '%d',
                'HH': '%H',
                'H24': '%H',
                'h12': '%I',
                'mm': '%M',
                'ss': '%S',
                'SSS': '%f',  # milliseconds
            }
            for k, v in mapping.items():
                format_str = format_str.replace(k, v)
            return format_str

        # Process the template
        pattern = re.compile(r'\$\{\s*(.*?)\s*\}')

        def extract_field_name(expr):
            expr = expr.strip()
            if expr.startswith('$'):
                expr = expr[1:]
            if expr.startswith('.'):
                expr = expr[1:]
            return expr

        def replace_placeholder(match, rec):
            expression = match.group(1)
            if ':' in expression:
                field_part, func_part = expression.split(':', 1)
                field_part = extract_field_name(field_part)
                func_part = func_part.strip()
                field_value = rec.get(field_part, '')
                if field_value == '':
                    self.logger.error(f'Field {field_part} not found in record')
                    return ''
                if func_part.startswith('date'):
                    try:
                        # Determine if field_value is an integer timestamp
                        if isinstance(field_value, int) or (isinstance(field_value, str) and field_value.isdigit()):
                            # Convert to integer
                            timestamp = int(field_value)
                            # Adjust for units
                            if timestamp_units == 'milliseconds':
                                timestamp = timestamp / 1000.0  # Convert to seconds
                            # Create datetime object from timestamp
                            date_obj = datetime.utcfromtimestamp(timestamp)
                        else:
                            # Assume field_value is a date string
                            parse_format = convert_date_format(input_date_format)
                            date_obj = datetime.strptime(field_value, parse_format)

                        # Format the output date
                        output_format = convert_date_format(output_date_format)
                        formatted_date = date_obj.strftime(output_format)
                        return formatted_date
                    except Exception as e:
                        self.logger.error(f'Error parsing or formatting date for field {field_part}: {str(e)}')
                        return ''
                else:
                    self.logger.error(f'Unknown function in template: {func_part}')
                    return ''
            else:
                field_name_inner = extract_field_name(expression)
                field_value = rec.get(field_name_inner, '')
                if field_value == '':
                    self.logger.error(f'Field {field_name_inner} not found in record')
                return str(field_value)

        def process_record(rec):
            try:
                substituted_string = pattern.sub(lambda m: replace_placeholder(m, rec), uuid_template)

                # Store the raw concatenated string for debugging
                if debug_field_name:
                    rec[debug_field_name] = substituted_string

                # Generate MD5 hash and UUID
                md5_hash = hashlib.md5(substituted_string.encode('utf-8')).hexdigest()
                generated_uuid = str(uuid.UUID(md5_hash))

                # Add the generated UUID to the specified field
                rec[field_name] = generated_uuid
            except Exception as e:
                self.logger.error(f'Error processing record: {str(e)}')
                # Optionally, you can route the record to failure here
                pass  # Continue processing other records even if one fails
            return rec

        # Check if the record is a list (array of objects)
        if isinstance(record, list):
            processed_records = []
            for rec in record:
                if isinstance(rec, dict):
                    processed_rec = process_record(rec)
                    processed_records.append(processed_rec)
                else:
                    self.logger.error('Invalid record type in array; expected dictionary')
            return RecordTransformResult(record=processed_records)
        elif isinstance(record, dict):
            processed_record = process_record(record)
            return RecordTransformResult(record=processed_record)
        else:
            self.logger.error('Invalid record type; expected dictionary or list of dictionaries')
            return RecordTransformResult(record=None, relationship='failure')
