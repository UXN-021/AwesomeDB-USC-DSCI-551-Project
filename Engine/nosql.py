import csv
import json
import operator
import os
import re
from Engine.base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE

class NoSQL(BaseEngine):
    def __init__(self):
        super().__init__()
    
    def run(self):
        print("NoSQL Database selected")
        while True:
            input_str = input("your query>").strip()
            if not self.parse_and_execute(input_str):
                break

    def drop_table(self, table_name: str) -> bool:
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return True
        table_storage_path = self._get_table_path(table_name)
        # delete the table directory
        for file in os.listdir(table_storage_path):
            os.remove(f"{table_storage_path}/{file}")
        os.rmdir(table_storage_path)
        print("table dropped")
        return True

    def load_data(self, file_name) -> bool:
        # check if file is csv
        if not file_name.endswith(".csv"):
            print("file must be a csv file")
            return True
        print("loading data...")
        csv_file_path = f"{BASE_DIR}/ToBeLoaded/{file_name}"
        table_name = file_name.split(".")[0]
        table_storage_path = f"{BASE_DIR}/Storage/NoSQL/{table_name}"
        # create the table directory if not exists
        if not os.path.exists(table_storage_path):
            os.mkdir(table_storage_path)
        else:
            print("Cannot load dataset. Table already exists!")
            return
        # read the first line of the csv to find the schema
        with open(csv_file_path, 'r') as f:
            csv_reader = csv.reader(f)
            table_schema = next(csv_reader)
            csv_row = next(csv_reader, None)
            while csv_row is not None:
                # convert csv row to json
                doc = self._csv_row_to_doc(csv_row, table_schema)
                # insert the doc into the table
                self._insert_doc(table_name, doc)
                csv_row = next(csv_reader, None)
        print("loading succeeded")
        return True
    
    def insert_data(self, table_name: str, data: list) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return True
        # convert the data to json
        doc = {}
        for field_data in data:
            field_name, field_value = field_data.split("=")
            # convert to correct type
            doc[field_name] = self._get_typed_value(field_value)
        # insert the doc into the table
        self._insert_doc(table_name, doc)
        print("insertion succeeded")
        return True
    
    def delete_data(self, table_name: str, condition: str) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            self._clear_file(chunk)
            filtered_docs = filter(lambda doc: not self._doc_meets_condition(doc, condition), docs)
            self._write_docs_to_file(filtered_docs, chunk)
        print("deletion succeeded")
        return True
    
    def update_data(self, table_name: str, condition: str, data: list) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            self._clear_file(chunk)
            for doc in docs:
                if self._doc_meets_condition(doc, condition):
                    for field_data in data:
                        field_name, field_value = field_data.split("=")
                        doc[field_name] = self._get_typed_value(field_value)
                self._write_doc_to_file(doc, chunk)
        print("update succeeded")
        return True
    
    def projection(self, table_name: str, fields: list) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            for doc in docs:
                projected_doc = {}
                for field in fields:
                    if field in doc:
                        projected_doc[field] = doc[field]
                self._print_doc(projected_doc)
        print("projection succeeded")
        return True
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For r/w json files 
    # ========================================================

    def _write_doc_to_file(self, doc: dict, file_path: str):
        with open(file_path, 'a') as f:
            f.write(json.dumps(doc) + "\n")

    def _write_docs_to_file(self, docs: list, file_path: str):
        with open(file_path, 'a') as f:
            for doc in docs:
                 f.write(json.dumps(doc) + "\n")

    def _read_docs_from_file(self, file_path: str) -> list:
        docs_data = []
        with open(file_path, 'r') as f:
            docs_data = [json.loads(line.rstrip("\n")) for line in f.readlines()]
        return docs_data
    
    def _next_doc(self, opened_file) -> dict or None:
        line = next(opened_file, None)
        if line is None:
            return None
        return json.loads(line.rstrip("\n"))
    
    def _clear_file(self, file_path: str) -> None:
        with open(file_path, 'r+') as f:
            f.truncate(0)

    def _get_typed_value(self, val: str) -> int or float or str:
        if val.isdigit():
            return int(val)
        elif val.replace(".", "", 1).isdigit():
            return float(val)
        else:
            return val
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                  For chunk management
    # ========================================================

    def _get_table_path(self, table_name: str) -> str:
        return f"{BASE_DIR}/Storage/NoSQL/{table_name}"
    
    def _table_exists(self, table_name: str) -> bool:
        table_storage_path = f"{BASE_DIR}/Storage/NoSQL/{table_name}"
        if not os.path.exists(table_storage_path):
            return False
        return True

    def _get_chunk_number(self, chunk_path: str) -> int:
        return int(chunk_path.split("/")[-1].split(".")[0].split("_")[-1])
    
    def _get_chunk_path(self, table_name: str, chunk_num: int) -> str:
        return f"{self._get_table_path(table_name)}/chunk_{chunk_num}"
    
    def _get_chunk_size(self, chunk_path: str) -> int:
        with open(chunk_path, 'r') as f:
            return len(f.readlines())
        
    def _get_table_chunks(self, table_name: str) -> list:
        table_storage_path = self._get_table_path(table_name)
        chunks = []
        for file in os.listdir(table_storage_path):
            chunks.append(f"{table_storage_path}/{file}")
        return chunks
        
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For doc operations
    # ========================================================

    def _csv_row_to_doc(self, row, schema) -> dict:
        doc = {}
        for i in range(len(schema)):
            doc[schema[i]] = self._get_typed_value(row[i])
        return doc
    
    def _insert_doc(self, table_name: str, doc: dict) -> None:
         # iterate through all chunks in this directory and find the chunk_num with max num, -1 if no chunks
        max_chunk_num = max([self._get_chunk_number(chunk) for chunk in self._get_table_chunks(table_name)], default=-1)
        if max_chunk_num == -1:
            # if no chunks, create a new chunk
            chunk_path = self._get_chunk_path(table_name, 0)
            self._write_doc_to_file(doc, chunk_path)
        else:
            # if chunks exist, find the chunk with max num
            chunk_path = self._get_chunk_path(table_name, max_chunk_num)
            # check if chunk is full
            if self._get_chunk_size(chunk_path) < CHUNK_SIZE:
                # if not full, append to the chunk
                self._write_doc_to_file(doc, chunk_path)
            else:
                # if full, create a new chunk
                chunk_path = self._get_chunk_path(table_name, max_chunk_num + 1)
                self._write_doc_to_file(doc, chunk_path)

    def _doc_meets_condition(self, doc: dict, condition: str) -> bool:
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        field, op, value = match.groups()
        value = self._get_typed_value(value)
        # check if doc has the field
        if field not in doc:
            return False
        # get the doc field value
        doc_field_value = doc[field]
        # check if doc field value is of the same type as value
        if type(doc_field_value) != type(value):
            return False
        # get the operator function
        ops = {
            "=": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
        }
        op_func = ops.get(op)
        # check if doc field value meets the condition
        return op_func(doc_field_value, value)
                
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For printing docs
    # ========================================================
    
    def _print_doc(self, doc: dict) -> None:
        print(json.dumps(doc, indent=4))
            