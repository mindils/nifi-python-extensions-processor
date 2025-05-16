from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import re
from jsonpath_ng import parse

class SQLGeneratorProcessorV5(FlowFileTransform):
    '''
    High‑performance SQL generator from JSON input using cached JSONPath compilation
    '''

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.1.0'
        description = 'High‑performance SQL generator from JSON input using cached JSONPath compilation'
        tags = ['sql', 'json', 'template', 'functions', 'fast']
        dependencies = ['jsonpath-ng']

    def __init__(self, **kwargs):
        super().__init__()
        # Pre‑compile the placeholder regex and create a cache for compiled JSONPath expressions
        self._placeholder_regex = re.compile(r'\$\{([^}]*)}')
        self._jsonpath_cache = {}

        self.descriptors = [
            PropertyDescriptor(
                name='SQL Template',
                description='Template string containing ${...} placeholders for JSON fields.',
                validators=[StandardValidators.NON_EMPTY_VALIDATOR],
                required=True
            ),
            PropertyDescriptor(
                name='Conditional Key',
                description='JSONPath expression to match against Conditional Value (e.g., $.status)',
                required=False
            ),
            PropertyDescriptor(
                name='Conditional Value',
                description='Value to match for Conditional Key (e.g., "active")',
                required=False
            ),
            PropertyDescriptor(
                name='Conditional SQL Template',
                description='Alternate SQL template to use when Conditional Key matches Conditional Value.',
                required=False
            ),
            PropertyDescriptor(
                name='Error Handling',
                description='How to handle errors: pass (return unchanged), original (rollback), failure (route to failure).',
                default_value='original',
                validators=[StandardValidators.NON_EMPTY_VALIDATOR]
            )
        ]

    # -------------------------------------------------------------------------
    # Property helpers
    # -------------------------------------------------------------------------
    def getPropertyDescriptors(self):
        return self.descriptors

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------
    def _get_expr(self, path):
        """Return a cached compiled JSONPath expression."""
        expr = self._jsonpath_cache.get(path)
        if expr is None:
            expr = parse(path)
            self._jsonpath_cache[path] = expr
        return expr

    def _read_json(self, flowfile):
        content_bytes = flowfile.getContentsAsBytes()
        return json.loads(content_bytes.decode('utf-8'))

    def _split_data(self, data, key_expr, value):
        if not key_expr or value is None:
            return [], data
        expr = self._get_expr(key_expr)
        if isinstance(data, list):
            matched = [item for item in data if any(m.value == value for m in expr.find(item))]
            unmatched = [item for item in data if item not in matched]
        else:
            matches = expr.find(data)
            matched = [data] if any(m.value == value for m in matches) else []
            unmatched = [] if matched else [data]
        return matched, unmatched

    def _render_all(self, items, template):
        if isinstance(items, list):
            return [self._render(template, item) for item in items]
        return [self._render(template, items)]

    def _render(self, template, context):
        return self._placeholder_regex.sub(lambda m: self._eval_placeholder(m.group(1), context), template)

    def _eval_placeholder(self, expr, context):
        expr = expr.strip()
        fn_match = re.match(r'^(\w+)\((.*)\)$', expr)
        if fn_match:
            func_name, args_str = fn_match.groups()
            args = [a.strip() for a in args_str.split(',', 1)]
            path = args[0]
            default = self._parse_default(args[1]) if len(args) > 1 else None
            value = self._extract(context, path, default)
            return self._apply_function(func_name, value)
        if ',' in expr:
            path, default_expr = [p.strip() for p in expr.split(',', 1)]
            default = self._parse_default(default_expr)
            value = self._extract(context, path, default)
        else:
            value = self._extract(context, expr)
        return self._format(value)

    @staticmethod
    def _parse_default(expr):
        expr = expr.strip()
        if expr.startswith("'") and expr.endswith("'"):
            return expr[1:-1]
        if expr.lower() == 'null':
            return None
        try:
            return json.loads(expr)
        except Exception:
            return expr

    def _extract(self, context, path, default=None):
        if path == '$':
            return context
        matches = self._get_expr(path).find(context)
        if not matches:
            return default
        values = [m.value for m in matches]
        return values[0] if len(values) == 1 else values

    def _apply_function(self, func_name, value):
        if value is None:
            return 'null'
        try:
            if func_name == 'toString':
                return "'" + str(value).replace("'", "''") + "'"
            if func_name == 'toInt':
                return str(int(value))
            if func_name == 'toJson':
                return "'" + json.dumps(value).replace("'", "''") + "'"
        except Exception:
            return 'null'
        return self._format(value)

    def _format(self, value):
        if value is None:
            return 'null'
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (dict, list)):
            return "'" + json.dumps(value).replace("'", "''") + "'"
        return "'" + str(value).replace("'", "''") + "'"

    # -------------------------------------------------------------------------
    # Main transform method
    # -------------------------------------------------------------------------
    def transform(self, context, flowfile):
        props = {d.name: context.getProperty(d.name).evaluateAttributeExpressions(flowfile).getValue() for d in self.descriptors}
        try:
            template = props.get('SQL Template')
            cond_key = props.get('Conditional Key')
            cond_val = props.get('Conditional Value')
            cond_template = props.get('Conditional SQL Template')
            data = self._read_json(flowfile)
            matched, unmatched = self._split_data(data, cond_key, cond_val)
            outputs = []
            if unmatched and template:
                outputs.extend(self._render_all(unmatched, template))
            if matched and cond_template:
                outputs.extend(self._render_all(matched, cond_template))
            return FlowFileTransformResult(
                relationship='success',
                contents='; '.join(outputs),
                attributes=flowfile.getAttributes()
            )
        except Exception as e:
            self.logger.error(str(e))
            return self._handle_error(flowfile, props.get('Error Handling', 'original').lower())

    # -------------------------------------------------------------------------
    # Error handling helper
    # -------------------------------------------------------------------------
    def _handle_error(self, flowfile, strategy):
        if strategy == 'pass':
            return FlowFileTransformResult('success', flowfile.getContentsAsBytes(), flowfile.getAttributes())
        if strategy == 'failure':
            return FlowFileTransformResult('failure')
        return FlowFileTransformResult('original')
