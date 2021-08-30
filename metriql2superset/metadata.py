from urllib.parse import urlparse, ParseResult


class MetriqlMetadata:
    _datasets: list
    _url: ParseResult

    def __init__(self, url: str, datasets):
        self._datasets = datasets
        self._url = urlparse(url)

    def get_datasets(self):
        return self._datasets

    def get_url(self):
        return self._url

    @staticmethod
    def get_dimension_for_column(dataset, column_name):
        for dimension in dataset.get('dimensions'):
            if dimension.get('type') == 'column' and dimension.get('value').get('column') == column_name:
                return dimension

    @staticmethod
    def default_aggregation_for_dimension(dataset, dimension):
        column_reference = None
        if dimension.get('type') == 'column':
            column_reference = dimension.get('value').get('column')

        for measure in dataset.get('measures'):
            field_value = measure.get('value')
            if measure.get('type') == 'dimension':
                if field_value.get('dimension') == dimension.get('name'):
                    return field_value.get('aggregation')
            if column_reference is not None and measure.get('type') == 'column':
                if field_value.get('column') == column_reference:
                    return field_value.get('aggregation')

    def get_dimensions(self, dataset_name):
        return self._get_fields(dataset_name, 'dimensions')

    def get_measures(self, dataset_name):
        return self._get_fields(dataset_name, 'measures')

    def _get_fields(self, dataset_name, field_type):
        fields = {}
        source_dataset = self.get_dataset(dataset_name)
        self._populate_fields(fields, source_dataset.get(field_type), None)

        for relation in source_dataset.get('relations'):
            relation_dataset = self.get_dataset(relation.get('modelName'))
            self._populate_fields(fields, relation_dataset.get(field_type), relation)

        return fields

    def get_dataset(self, name):
        datasets = list(filter(lambda d: d.get('name') == name, self._datasets))
        if len(datasets) == 0:
            raise Exception("dataset {} not found", name)
        return datasets[0]

    @staticmethod
    def _get_field_by_name(fields, name):
        return next(filter(lambda d: d.get('name') == name, fields))

    @staticmethod
    def _populate_fields(fields, dataset_fields, relation):
        field_prefix = "{}.".format(relation.get('name')) if relation is not None else ""
        for field in dataset_fields:
            fields[field_prefix + field.get('name')] = field, relation
