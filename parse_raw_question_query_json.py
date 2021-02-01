import os, sys
import json
import sqlite3
import traceback
import argparse
from process_sql import get_sql, tokenize

# Set the correct paths below
sql_path = 'question_query_pairs_raw.json'
output_file = 'dev_new.json'
table_file = 'tables.json'


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema, table):
        self._schema = schema
        self._table = table
        self._idMap = self._map(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema, table):
        column_names_original = table['column_names_original']
        table_names_original = table['table_names_original']
        for i, (tab_id, col) in enumerate(column_names_original):
            if tab_id == -1:
                idMap = {'*': i}
            else:
                key = table_names_original[tab_id].lower()
                val = col.lower()
                idMap[key + "." + val] = i

        for i, tab in enumerate(table_names_original):
            key = tab.lower()
            idMap[key] = i

        return idMap


def get_schemas_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)
    db_names = [db['db_id'] for db in data]

    tables = {}
    schemas = {}
    for db in data:
        db_id = db['db_id']
        schema = {} #{'table': [col.lower, ..., ]} * -> __all__
        column_names_original = db['column_names_original']
        table_names_original = db['table_names_original']
        tables[db_id] = {'column_names_original': column_names_original, 'table_names_original': table_names_original}
        for i, tabn in enumerate(table_names_original):
            table = str(tabn.lower())
            cols = [str(col.lower()) for td, col in column_names_original if td == i]
            schema[table] = cols
        schemas[db_id] = schema

    return schemas, db_names, tables



schemas, db_names, tables = get_schemas_from_json(table_file)

with open(sql_path) as inf:
    sql_data = json.load(inf)

sql_data_new = []
for data in sql_data:
    try:
        db_id = data["db_id"]
        table = tables[db_id]
        schema = Schema(schemas[db_id], table)
        sql = data["query"]
        sql_label, toks = get_sql(schema, sql)
        data["sql"] = sql_label
        data["query_toks"] = toks
        data["question_toks"] = data['question'].split()
        sql_data_new.append(data)
    except:
        print("db_id: ", db_id)
        print("sql: ", sql)

print('\n', len(sql_data_new), 'of', len(sql_data), 'converted.')

with open(output_file, 'wt') as out:
    json.dump(sql_data_new, out, sort_keys=True, indent=4, separators=(',', ': '))
