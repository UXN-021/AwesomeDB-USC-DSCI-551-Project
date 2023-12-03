from .base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE
import os
import re
import operator

class Relational(BaseEngine):
    def __init__(self):
        super().__init__()

    def show_tables(self):
        print("show tables")

    def create_table(self, database_name, fields):
        print("create table")

    def insert_data(self, table_name, data):
        data_fields = data.split(",")
        table_schema = self._get_table_schema(table_name)
        # check if the data fields are in the table schema
        data_dict = {}
        for data_field in data_fields:
            field_name, field_value = data_field.split("=")
            if field_name not in table_schema:
                raise Exception("field name not in table schema")
            data_dict[field_name] = field_value
            print(field_name, field_value)
        
        # form the new row to insert
        row = ""
        for field in table_schema:
            print(field)
            row = row + data_dict.get(field, "") + ","
        row = row[:-1] # remove the last comma
        # insert the new row
        self._insert_row(table_name, row)
        




        
        
        

    def delete_data(self, table_name, condition):
        table_schema = self._get_table_schema(table_name)
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # iterate through all .csv file and delete rows that meet the condition
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r+") as f:
                    lines = f.readlines()
                    f.truncate(0)
                with open(f"{table_storage_path}/{file}", "w") as f:
                    for line in lines:
                        line = line.rstrip("\n")
                        if not self._row_meets_condition(table_schema, line, condition):
                            f.write(line + "\n")

    
    
        

    def _row_meets_condition(self, schema, row, condition):
        # !!! issue: only support one condition
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        field, op, value = match.groups()

        if value.isdigit():
            value = int(value)
        elif value.replace('.', '', 1).isdigit():
            value = float(value)

        ops = {
            "=": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
        }

        op_func = ops.get(op)
        if not op_func:
            return False
        
        # get the index of the field
        field_index = schema.index(field)
        # get the value of the field
        row_value = row.split(",")[field_index]
        if row_value.isdigit():
            row_value = int(row_value)
        elif row_value.replace('.', '', 1).isdigit():
            row_value = float(row_value)
        
        return op_func(row_value, value)
        

    def update_data(self, table_name, condition, data):
        
        
        table_schema = self._get_table_schema(table_name)
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # iterate through all .csv file and delete rows that meet the condition
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r+") as f:
                    lines = f.readlines()
                    f.truncate(0)
                with open(f"{table_storage_path}/{file}", "w") as f:
                    for line in lines:
                        line = line.rstrip("\n")
                        if self._row_meets_condition(table_schema, line, condition):
                            # meet the condition, update the row
                            data_dict = {} # key: field name, value: field value
                            # copy original values into data_dict
                            original_values = line.split(",")
                            for field in table_schema:
                                data_dict[field] = original_values[table_schema.index(field)]
                            # update the values in data_dict
                            update_fields = data.split(",")
                            for update_field in update_fields:
                                update_field_name, update_value = update_field.split("=")
                                data_dict[update_field_name] = update_value
                            # form the new row to insert
                            row = ""
                            for field in table_schema:
                                row = row + data_dict.get(field, "") + ","
                            row = row[:-1] # remove the last comma
                            # insert the new row
                            f.write(row + "\n")
                        else:
                            f.write(line + "\n")

    def projection(self, fields, table_name):
        print("projection")

    def filtering(self, fields, table_name, condition):
        print("filtering")

    def join(self, table_name1, table_name2, condition):
        print("join")

    def aggregate(self, fields, table_name, condition):
        print("aggregate")

    def order(self, table_name, field, order):
        print("order")


    def run(self):
        print("Relational Database selected")
        while True:
            input_str = input("your query>").strip()
            if not self.parse_and_execute(input_str):
                break
    
    # return a tuple
    def _get_table_schema(self, table_name):
        schema_path = f"{BASE_DIR}/Storage/Relational/{table_name}/schema.txt"
        with open(schema_path, "r") as f:
            schema = f.read()
        return tuple(schema.split(","))
    
    def load_data(self, file_name):
        csv_file_path = f"{BASE_DIR}/ToBeLoaded/{file_name}"
        table_name = file_name.split(".")[0]
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # create the table directory if not exists
        if not os.path.exists(table_storage_path):
            os.mkdir(table_storage_path)
            print("Directory created:", table_storage_path)
        else:
            print("Table already exists!")
            return
        # read the first line of the csv to find the schema
        with open(csv_file_path, "r") as f:
            table_schema = f.readline().rstrip("\n")
            # write the schema to the schema.txt
            with open(f"{table_storage_path}/schema.txt", "w") as f:
                f.write(table_schema)
        
        # load the rest of the data to the storage using _insert_row
        with open(csv_file_path, "r") as lines:
            next(lines)
            for line in lines:
                self._insert_row(table_name, line.rstrip("\n"))


    def _insert_row(self, table_name, row):
        # insert the new row
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # iterate through all .csv files in this directory and find the chunk_num with max num
        max_chunk_num = -1
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                chunk_num = int(file.split(".")[0].split("_")[1])
                if chunk_num > max_chunk_num:
                    max_chunk_num = chunk_num
        if max_chunk_num == -1:
            # create a new csv chunk
            with open(f"{table_storage_path}/chunk_0.csv", "w") as f:
                f.write(row + "\n")
        else:
            # check if the last chunk is full
            with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "r") as f:
                lines = f.readlines()
                if len(lines) < CHUNK_SIZE:
                    # append to the last chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "a") as f:
                        # !!! issue: always create a new line
                        f.write(row + "\n")
                else:
                    # create a new chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num + 1}.csv", "w") as f:
                        f.write(row + "\n")

