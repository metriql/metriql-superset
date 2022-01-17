import json
from bs4 import BeautifulSoup
import requests
from requests import Session

direct_mapping_types = ['boolean', 'integer', 'boolean', 'date']

datetime_format = {

}


class DatabaseOperation:
    superset_url: str
    session: Session

    def __init__(self, superset_url, username, password) -> None:
        self.superset_url = superset_url

        # set up session for auth
        self.session = requests.Session()
        self.session.headers["Referer"] = self.superset_url

        ## TODO: check hostname
        if '.app.preset.io/' in superset_url.lower():
            self.login_preset(username, password)
        else:
            self.login_self_hosted_superset(username, password)

    def login_preset(self, token, secret):
        preset_auth = self.session.post('https://manage.app.preset.io/api/v1/auth/',
                                        json={"name": token, "secret": secret})
        if preset_auth.status_code != 200:
            raise Exception("Unable to login: {}".format(preset_auth.text))
        access_token = preset_auth.json().get('payload').get('access_token')
        self.session.headers["Authorization"] = "Bearer {}".format(access_token)

    def login_self_hosted_superset(self, username, password):
        login_form = self.session.get("{}/login/".format(self.superset_url))

        # get Cross-Site Request Forgery protection token
        soup = BeautifulSoup(login_form.text, 'html.parser')
        csrf_token = soup.find('input', {'id': 'csrf_token'})['value']
        if csrf_token is None:
            raise Exception("csrf_token could not found.")

        # login the given session
        login_request = self.session.post('{}/login/'.format(self.superset_url),
                                          data=dict(username=username, password=password, csrf_token=csrf_token))
        self.session.headers["X-CSRFToken"] = csrf_token

        if login_request.status_code != 200:
            raise Exception("Unable to login: {}".format(login_request.text))

    # Superset Dataset update API doesn't support JWT so we don't use this function at the moment
    def setup_access_token(self):
        r = requests.post("{}/api/v1/security/login".format(self.superset_url), json={
            "password": self.password,
            "provider": "db",
            "refresh": True,
            "username": self.username
        })
        if r.status_code != 200:
            raise Exception("Unable to authenticate: {}".format(r.text))

        info = r.json()
        self.current_access_token = info.get('access_token')
        self.refresh_token = info.get('refresh_token')
        self.csrf_token = self._create_csrf_token()

    def _refresh_token(self):
        r = requests.post("{}/api/v1/security/refresh".format(self.superset_url), self.refresh_token)

        if r.status_code != 200:
            raise Exception("Unable to authenticate: {}".format(r.text))

        info = r.json()
        self.current_access_token = info.get('access_token')

    def list(self):
        r = self.session.get("{}/api/v1/database".format(self.superset_url))

        if r.status_code == 401:
            raise Exception("Invalid credentials")
        elif r.status_code == 404:
            raise Exception("Invalid Superset URL, 404 returned")
        elif r.status_code != 200:
            raise Exception("Unable to perform operation: {}".format(r.text))

        all_databases = map(lambda db: {"name": db.get('database_name'), "id": db.get('explore_database_id'),
                                        "backend": db.get('backend')}, r.json().get('result'))
        return list(filter(lambda db: db.get('backend') == 'trino', all_databases))

    def _create_csrf_token(self):
        token_request = requests.get("{}/api/v1/security/csrf_token".format(self.superset_url),
                                     headers={'Authorization': 'Bearer {}'.format(self.current_access_token)})
        if token_request.status_code != 200:
            raise Exception("Unable to get CSRF token: {}".format(token_request.text))
        return token_request.json().get('result')

    def create(self, metriql_url, database_name):
        raise Exception("Operation is not  implemented. See: https://github.com/apache/superset/issues/16398")

        protocol = "https"
        uri = "trino://TODO:TODO@{}:443/metriql?protocol={}".format(metriql_url, protocol)
        extra = {"engine_params": {"connect_args": {"http_scheme": "https"}}}
        r = self.session.post("{}/api/v1/database".format(self.superset_url),
                              json={
                                  "allow_csv_upload": False,
                                  "allow_ctas": True,
                                  "allow_cvas": False,
                                  "allow_dml": False,
                                  "allow_multi_schema_metadata_fetch": True,
                                  "allow_run_async": True,
                                  "extra": json.dumps(extra),
                                  "extra_json": extra,
                                  "database_name": database_name,
                                  "expose_in_sqllab": True,
                                  "sqlalchemy_uri": uri
                              })

        if r.status_code == 401:
            raise Exception("Invalid credentials")
        elif r.status_code != 200:
            raise Exception("Unable to perform operation: {}".format(r.text))

        return self.list()

    @staticmethod
    def _get_column_datatype(field_type):
        if field_type is None:
            return "string"

        if field_type in ['string', 'boolean']:
            return field_type

        if field_type in ['double', 'long', 'integer']:
            return 'numeric'

        if field_type in ['timestamp', 'date', 'time']:
            return 'datetime'

        raise Exception("Unknown type {}".format(field_type))

    @staticmethod
    def get_python_date_format(field_type):
        return None

    @staticmethod
    def _build_metrics(measures, existing_metrics):
        existing_metric_lookup = {}
        for metric in existing_metrics:
            existing_metric_lookup[metric.get('metric_name')] = metric.get('id')

        return [{
            "d3format": ((measure.get('reportOptions') or {}).get('superset') or {}).get('d3_format'),
            "description": measure.get('description'),
            "expression": '"{}"'.format(name.replace('"', '""')),
            "id": name,
            "metric_name": name,
            "verbose_name": measure.get('label'),
        } for (name, (measure, relation)) in measures.items()]

    @staticmethod
    def _build_columns(dimensions, measures, mappings, existing_columns):
        existing_column_lookup = {}
        for metric in existing_columns:
            existing_column_lookup[metric.get('column_name')] = metric.get('id')

        columns = []

        for name, (dimension, relation) in dimensions.items():
            column_id = existing_column_lookup.get(name)
            if dimension.get('postOperations') is not None:
                for operation in dimension.get('postOperations'):
                    label = "{} ({})".format(dimension.get('label') or name, operation)
                    columns.append(DatabaseOperation._build_column(column_id, name+'::'+operation, dimension, True, mappings, label))
            else:
                columns.append(DatabaseOperation._build_column(column_id, name, dimension, True, mappings, dimension.get('label')))

        return columns

    @staticmethod
    def _build_column(column_id, name, field, is_dimension, mappings, label):
        datatype = DatabaseOperation._get_column_datatype(field.get('fieldType'))
        return {
            "column_name": name,
            "description": field.get('description'),
            "filterable": True,
            "groupby": is_dimension,
            "id": name,
            "is_active": not field.get('hidden'),
            "is_dttm": datatype == 'datetime' if is_dimension else False,
            "python_date_format": DatabaseOperation.get_python_date_format(field.get('fieldType')),
            "type": datatype,
            "verbose_name": label
        }

    def sync(self, database_id, metadata):
        superset_dataset_requests = self.session.get("{}/api/v1/dataset".format(self.superset_url),
                                                     params={"page_size": "100000",
                                                             "keys": ['sql', 'table_name', 'schema', 'id', 'metrics',
                                                                      'columns'],
                                                             "filters": [{"col": "database", "opr": "rel_m_m",
                                                                          "value": database_id}]})

        if superset_dataset_requests.status_code == 401:
            raise Exception("Invalid credentials")
        elif superset_dataset_requests.status_code != 200:
            raise Exception("Unable to perform operation: {}".format(superset_dataset_requests.text))

        superset_datasets = superset_dataset_requests.json().get('result')
        metriql_datasets = metadata.get_datasets()

        for dataset in metriql_datasets:
            table_schema = "public" if dataset.get('category') is None else dataset.get('category')
            table_name = dataset.get('name')
            mappings = dataset.get('mappings')

            # find existing dataset
            existing_dataset = next(
                filter(lambda db: db.get('schema') == table_schema and db.get('table_name') == table_name,
                       superset_datasets), None)
            existing_dataset_id = existing_dataset.get('id') if existing_dataset is not None else None

            existing_columns = []
            existing_metrics = []
            new_dataset_count = 0
            if existing_dataset is not None:
                superset_dataset_content = self.session.get(
                    "{}/api/v1/dataset/{}".format(self.superset_url, existing_dataset_id),
                    params={
                        "columns": [
                            "columns", "metrics"
                        ],
                        "keys": [
                            "show_columns"
                        ]
                    })
                if superset_dataset_content.status_code != 200:
                    raise Exception("Unable to get dataset {}.{}: {}".format(table_schema, table_name,
                                                                             superset_dataset_content.text))
                content = superset_dataset_content.json().get('result')
                existing_columns = content.get('columns')
                existing_metrics = content.get('metrics')
                existing_dataset_id = content.get('id')
            else:
                new_dataset_count += 1

            dimensions = metadata.get_dimensions(table_name)
            measures = metadata.get_measures(table_name)

            metrics = DatabaseOperation._build_metrics(measures, existing_metrics)
            columns = DatabaseOperation._build_columns(dimensions, measures, mappings, existing_columns)

            if existing_dataset is None:
                created_dataset_request = self.session.post(
                    "{}/api/v1/dataset".format(self.superset_url), json={"database": database_id,
                                                                         "schema": table_schema,
                                                                         "table_name": table_name,
                                                                         # "owners": [
                                                                         #     0
                                                                         # ],
                                                                         })
                if created_dataset_request.status_code not in [201, 200]:
                    raise Exception("Unable to create dataset: {}".format(created_dataset_request.text))
                existing_dataset_id = created_dataset_request.json().get('id')

            updated_dataset_request = self.session.post(
                "{}/datasource/save".format(self.superset_url), files=[], data={"data": json.dumps({
                    "id": existing_dataset_id,
                    "columns": columns,
                    # "datasource_type": "table",
                    "type": "table",
                    "database": {"id": database_id},
                    "description": dataset.get('description'),
                    "filter_select_enabled": True,
                    "is_sqllab_view": False,
                    "main_dttm_col": mappings.get('event_timestamp'),
                    "metrics": metrics,
                    # "owners": [
                    #     0
                    # ],
                    "schema": table_schema,
                    "table_name": table_name
                })})

            if updated_dataset_request.status_code != 200:
                raise Exception("Unable to update dataset: {}".format(updated_dataset_request.text))

            print(dataset.get('name'))

        print("Successfully synchronized existing {} datasets, created {} datasets".format(
            len(metriql_datasets) - new_dataset_count, new_dataset_count))
