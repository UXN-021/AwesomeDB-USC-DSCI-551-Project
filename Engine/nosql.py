import csv
import json
import os
from Engine.base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE

# same as relational.py but store data in json format
class NoSQL(BaseEngine):
    def __init__(self):
        super().__init__()
    
    def run(self):
        print("NoSQL Database selected")
        while True:
            input_str = input("your query>").strip()
            if not self.parse_and_execute(input_str):
                break

    def load_data(self, file_name) -> bool:
        # check if file is csv
        if not file_name.endswith(".csv"):
            print("file must be a csv file")
            return False
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
                doc = self._csv_row_to_json(csv_row, table_schema)
                # insert the doc into the table
                self._insert_doc(table_name, doc)
                csv_row = next(csv_reader, None)
        print("loading succeeded")
        return True
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For r/w json files 
    # ========================================================
    def _write_doc_to_file(self, doc, file_path):
        with open(file_path, 'a') as f:
            f.write(doc + "\n")

    def _write_docs_to_file(self, docs, file_path):
        with open(file_path, 'a') as f:
            for doc in docs:
                f.write(doc + "\n")

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
    def _csv_row_to_dict(self, row, schema) -> dict:
        row_dict = {}
        for i in range(len(schema)):
            row_dict[schema[i]] = self._get_typed_value(row[i])
        return row_dict
    
    def _csv_row_to_json(self, row, schema) -> str:
        row_dict = self._csv_row_to_dict(row, schema)
        return json.dumps(row_dict)
    
    def _insert_doc(self, table_name: str, doc: str) -> None:
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
                
            
            
            