# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0


from pyhocon import ConfigTree
from typing import Any, Dict, Iterator, Union

from databuilder.extractor.generic_extractor import GenericExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class ElasticsearchIndexExtractor(GenericExtractor):
    """
    Extractor to extract index metadata from Elasticsearch
    """

    ELASTICSEARCH_CLIENT_CONFIG_KEY = 'client'

    PRODUCT = 'product'
    CLUSTER = 'cluster'
    SCHEMA = 'schema'

    def __init__(self) -> None:
        super(ElasticsearchIndexExtractor, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self._extract_iter = self._get_extract_iter()

        self.es = self.conf.get(ElasticsearchIndexExtractor.ELASTICSEARCH_CLIENT_CONFIG_KEY)

    def _get_indexes(self) -> Dict:
        result = dict()

        try:
            _indexes = self.es.indices.get('*')

            for k, v in _indexes.items():
                if not k.startswith('.'):
                    result[k] = v
        except:
            pass

        return result

    def extract(self) -> Any:
        try:
            result = next(self._extract_iter)

            return result
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.es_indexes'

    @property
    def database(self) -> str:
        return 'elasticsearch'

    @property
    def cluster(self) -> str:
        return self.conf.get(ElasticsearchIndexExtractor.CLUSTER)

    @property
    def schema(self) -> str:
        return self.conf.get(ElasticsearchIndexExtractor.SCHEMA)

    def _get_extract_iter(self) -> Iterator[Union[TableMetadata, None]]:
        indexes: Dict = self._get_indexes()

        for index_name, index_metadata in indexes.items():
            mappings = index_metadata.get('mappings', dict())

            doc_type = list(mappings.keys())[0] if mappings else '-1'

            properties = mappings.get(doc_type, dict()).get('properties', dict()) or dict()

            columns = []

            for column_name, column_metadata in properties.items():
                column_metadata = ColumnMetadata(name=column_name,
                                                 description='',
                                                 col_type=column_metadata.get('type', ''),
                                                 sort_order=0)
                columns.append(column_metadata)

            table_metadata = TableMetadata(database=self.database,
                                           cluster=self.cluster,
                                           schema=self.schema,
                                           name=index_name,
                                           description='',
                                           columns=columns,
                                           is_view=False,
                                           tags=None,
                                           description_source=None)

            yield table_metadata
