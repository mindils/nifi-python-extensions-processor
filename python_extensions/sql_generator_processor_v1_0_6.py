from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json
import re
from jsonpath_ng import parse
from nifiapi.properties import PropertyDescriptor, StandardValidators

class SQLGeneratorProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '1.0.6'
        description = 'Generates SQL queries by replacing placeholders with values from JSON using JSONPath. Supports conditional SQL based on JSON key-value matching.'
        tags = ['SQL', 'JSON', 'Template']
        dependencies = ['jsonpath-ng']

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        sql_template = PropertyDescriptor(
            name='SQL Template',
            description='SQL template with placeholders to be replaced with values from JSON. Use JSONPath expressions inside ${...}.',
            required=True,
            default_value='',
        )

        store_in_content = PropertyDescriptor(
            name='Store in Content',
            description='Indicates whether to place the SQL in the FlowFile content (true) or in an attribute (false).',
            required=False,
            default_value='false',
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )

        attribute_name = PropertyDescriptor(
            name='Attribute Name',
            description='Name of the attribute where the generated SQL will be placed if storing in an attribute.',
            required=False,
            default_value='generated_sql',
        )

        conditional_key = PropertyDescriptor(
            name='Conditional Key',
            description='Name of the key in JSON to check for a specific value.',
            required=False,
            default_value='',
        )

        conditional_value = PropertyDescriptor(
            name='Conditional Value',
            description='The value that the Conditional Key should have to use the Conditional SQL Template.',
            required=False,
            default_value='',
        )

        conditional_sql_template = PropertyDescriptor(
            name='Conditional SQL Template',
            description='SQL template to use if the Conditional Key has the Conditional Value. Use JSONPath expressions inside ${...}.',
            required=False,
            default_value='',
        )

        error_handling = PropertyDescriptor(
            name='Error Handling',
            description=('Specifies how to handle errors:\n'
                         '1. "pass": Log the error and pass the FlowFile downstream.\n'
                         '2. "original": !!!NOT WORK transfer to failure !!! Return the FlowFile to the original queue (default).\n'
                         '3. "failure": Route the FlowFile to the failure relationship.\n'
                         'If an unknown value is provided, defaults to "original".'),
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
        # Get the properties
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
        error_handling = (error_handling_prop.evaluateAttributeExpressions(flowfile).getValue() or 'return').lower()

        # Read FlowFile content and parse JSON
        content_bytes = flowfile.getContentsAsBytes()
        content_str = content_bytes.decode('utf-8')
        try:
            json_data = json.loads(content_str)
        except Exception as e:
            self.logger.error('Processing failed: {}'.format(e))
            return self.handle_error(flowfile, error_handling)

        # Determine which SQL template to use
        use_conditional_sql = False
        if conditional_key and conditional_value and conditional_sql_template:
            try:
                # Evaluate the value of the conditional key using JSONPath
                actual_value = self.evaluate_expression(conditional_key, json_data)
                if actual_value == conditional_value:
                    use_conditional_sql = True
                    sql_template_to_use = conditional_sql_template
                else:
                    sql_template_to_use = sql_template
            except Exception as e:
                self.logger.error('Error evaluating Conditional Key: {}'.format(e))
                return self.handle_error(flowfile, error_handling)
        else:
            sql_template_to_use = sql_template

        if not sql_template_to_use:
            self.logger.error('SQL Template is not set.')
            return self.handle_error(flowfile, error_handling)

        # Function to replace placeholders
        def replace_placeholder(match):
            expression = match.group(1)
            try:
                value = self.evaluate_expression(expression, json_data)
                return value
            except Exception as e:
                self.logger.error('Error processing expression {}: {}'.format(expression, e))
                raise  # Trigger error handling

        try:
            # Replace placeholders in the selected SQL template
            sql_generated = re.sub(r'\$\{([^}]+)\}', replace_placeholder, sql_template_to_use)
        except Exception as e:
            return self.handle_error(flowfile, error_handling)

        if store_in_content:
            # Place generated SQL in the FlowFile content
            return FlowFileTransformResult(
                relationship='success',
                contents=sql_generated,
                attributes=flowfile.getAttributes()
            )
        else:
            # Create a new attributes dictionary
            attributes = dict(flowfile.getAttributes())  # Copy existing attributes
            attributes[attribute_name] = sql_generated  # Add the generated SQL
            return FlowFileTransformResult(
                relationship='success',
                attributes=attributes
            )

    def handle_error(self, flowfile, error_handling):
        """
        Handle errors based on the error handling strategy.
        """
        if error_handling == 'pass':
            # Log error but pass the FlowFile downstream
            self.logger.warn("Error occurred, passing FlowFile to success relationship.")
            return FlowFileTransformResult(relationship='success')
        elif error_handling == 'failure':
            # Route to failure relationship
            return FlowFileTransformResult(relationship='failure')
        else:
            # Default: route back to original queue by raising an exception
            self.logger.warn("Error occurred, returning FlowFile to original queue.")
            raise Exception("Error occurred, returning FlowFile to original queue.")

    def evaluate_expression(self, expression, json_data):
        # Parse JSONPath expression
        jsonpath_expr = parse(expression)
        matches = [match.value for match in jsonpath_expr.find(json_data)]
        if not matches:
            raise ValueError('No matches for expression: {}'.format(expression))
        # If multiple values, join them with commas
        if len(matches) > 1:
            return ','.join(str(m) for m in matches)
        else:
            return str(matches[0])

    def get_value_from_json(self, key, json_data):
        # Supports dot notation for nested keys (e.g., 'person.name')
        keys = key.split('.')
        value = json_data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        return str(value)
