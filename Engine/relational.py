from utils.RowElement import RowElement
from utils.util import get_format_str, print_row, print_table_header
from .base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE, FIELD_PRINT_LEN
import os
import re
import operator
import csv
from queue import PriorityQueue

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
        row = []
        for field in table_schema:
            row.append(data_dict.get(field, ""))
        # insert the new row
        self._insert_row(table_name, row)
        




        
        
        

    def delete_data(self, table_name, condition):
        table_schema = self._get_table_schema(table_name)
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # iterate through all .csv file and delete rows that meet the condition
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r+") as f:
                    csv_reader = csv.reader(f)
                    rows = list(csv_reader)
                    f.truncate(0)
                with open(f"{table_storage_path}/{file}", "w") as f:
                    csv_writer = csv.writer(f)
                    for row in rows:
                        if not self._row_meets_condition(table_schema, row, condition):
                            csv_writer.writerow(row)
                            

    
    
        

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
        row_value = row[field_index]
        if type(value) is int:
            row_value = int(row_value)
        elif type(value) is float:
            if row_value == "":
                row_value = float("-inf")
            row_value = float(row_value)
        
        return op_func(row_value, value)
        

    def update_data(self, table_name, condition, data):
        
        
        table_schema = self._get_table_schema(table_name)
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # iterate through all .csv file and delete rows that meet the condition
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r+") as f:
                    csv_reader = csv.reader(f)
                    rows = list(csv_reader)
                    f.truncate(0)
                with open(f"{table_storage_path}/{file}", "w") as f:
                    csv_writer = csv.writer(f)
                    for row in rows:
                        if self._row_meets_condition(table_schema, row, condition):
                            # meet the condition, update the row
                            data_dict = {} # key: field name, value: field value
                            # copy all old values into data_dict
                            old_row = row
                            for field in table_schema:
                                data_dict[field] = old_row[table_schema.index(field)]
                            # update the values in data_dict
                            update_fields = data.split(",")
                            for update_field in update_fields:
                                update_field_name, update_value = update_field.split("=")
                                data_dict[update_field_name] = update_value
                            # form the new row to insert
                            new_row = []
                            for field in table_schema:
                                new_row.append(data_dict.get(field, ""))
                            # insert the new row
                            csv_writer.writerow(new_row)
                        else:
                            csv_writer.writerow(row)

    def projection(self, table_name, fields):
        # check if the fields are in the table schema
        table_schema = self._get_table_schema(table_name)
        for field in fields.split(","):
            if field not in table_schema:
                raise Exception(f"field {field} not in table schema")
            
        # create a schema for the projection table
        projection_schema = []
        for field in fields.split(","):
            projection_schema.append(field)
            
        # get the format string for printing
        format_str = get_format_str(projection_schema, FIELD_PRINT_LEN)
        # print the header
        print_table_header(projection_schema, format_str)
        
        # iterate through all .csv file and print the specified fields to console
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r") as f:
                    csv_reader = csv.reader(f)
                    for row in csv_reader:
                        row_dict = {}
                        for field in table_schema:
                            row_dict[field] = row[table_schema.index(field)]
                        # print the row
                        print_row(row_dict, projection_schema, format_str, FIELD_PRINT_LEN)
                        
        

    def filtering(self, table_name, fields, condition):
        # check if the fields are in the table schema
        table_schema = self._get_table_schema(table_name)
        for field in fields.split(","):
            if field not in table_schema:
                print(table_schema)
                raise Exception(f"field {field} not in table schema")
            
        # create a schema for the projection table
        projection_schema = []
        for field in fields.split(","):
            projection_schema.append(field)
            
        # get the format string for printing
        format_str = get_format_str(projection_schema, FIELD_PRINT_LEN)
        # print the header
        print_table_header(projection_schema, format_str)
        
        # iterate through all .csv file and print the specified fields to console
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                with open(f"{table_storage_path}/{file}", "r") as f:
                    csv_reader = csv.reader(f)
                    for row in csv_reader:
                        # check if the row meets the condition
                        if not self._row_meets_condition(table_schema, row, condition):
                            continue
                        row_dict = {}
                        for field in table_schema:
                            row_dict[field] = row[table_schema.index(field)]
                        # print the row
                        print_row(row_dict, projection_schema, format_str, FIELD_PRINT_LEN)

    # use right table as outter table
    # def join(self, left, right, condition):
    #     left_schema = self._get_table_schema(left)
    #     right_schema = self._get_table_schema(right)
    #     # extract the fields from the condition
    #     match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
    #     left_field, op, right_field = match.groups()
    #     # check if the fields are in the table schema
    #     if left_field not in left_schema or right_field not in right_schema:
    #         raise Exception(f"field {left_field} not in table schema")
    #     # get the index of the fields
    #     left_field_index = left_schema.index(left_field)
    #     right_field_index = right_schema.index(right_field)
    #     # for each chunk in the right table, iterate through all rows in the left table
    #     # and output matching rows to console
    #     left_table_storage_path = f"{BASE_DIR}/Storage/Relational/{left}"
    #     right_table_storage_path = f"{BASE_DIR}/Storage/Relational/{right}"
    #     for right_file in os.listdir(right_table_storage_path):
    #         if right_file.endswith(".csv"):
    #             with open(f"{right_table_storage_path}/{right_file}", "r") as right_f:
    #                 right_csv_reader = csv.reader(right_f)
    #                 right_row = next(right_csv_reader, None)
    #                 while right_row is not None:
    #                     right_field_value = right_row[right_field_index]
    #                     # convert the condition id=id to id=4 for the left table
    #                     new_condition = f"{left_field}{op}{right_field_value}"
    #                     for left_file in os.listdir(left_table_storage_path):
    #                         if left_file.endswith(".csv"):
    #                             with open(f"{left_table_storage_path}/{left_file}", "r") as left_f:
    #                                 line = left_f.readline().rstrip("\n")

        
    #     pass

    def aggregate(self, fields, table_name, condition):
        print("aggregate")

    def order(self, table_name, field, order_method):
        # check if the field is in the table schema
        table_schema = self._get_table_schema(table_name)
        if field not in table_schema:
            raise Exception(f"field {field} not in table schema")
        # find the index of the field
        field_index = table_schema.index(field)
        field_type = None
        # creates temporary sorted csv table in the Temp directory
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                # sort the csv file based on the field
                with open(f"{table_storage_path}/{file}", "r") as f:
                    csv_reader = csv.reader(f)
                    # determine the field type
                    # !!! issue: weak typing
                    if field_type is None:
                        for row in csv_reader:
                            if row[field_index].isdigit():
                                field_type = int
                                break
                            elif row[field_index].replace('.', '', 1).isdigit():
                                field_type = float
                                break
                            else:
                                field_type = str
                                break
                    # get the value of the field
                    def get_key(row):
                        if field_type == int:
                            return int(row[field_index])
                        elif field_type == float:
                            return float(row[field_index]) if row[field_index] != "" else float("-inf")
                        else:
                            return row[field_index]
                    sorted_table = sorted(csv_reader, key = lambda row: get_key(row), reverse = True if order_method == "desc" else False)
                # write the sorted table to the Temp directory
                chunk_num = file.split(".")[0].split("_")[1]
                with open(f"{BASE_DIR}/Temp/chunk_{chunk_num}_pass_0.csv", "w") as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerows(sorted_table)
        # merge the sorted chunks
        merged_file = self._merge_sorted_chunks(field, table_schema, order_method, 0)
        # print the merged file
        format_str = get_format_str(table_schema, FIELD_PRINT_LEN)
        print_table_header(table_schema, format_str)
        with open(merged_file, "r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                row_dict = {}
                for field in table_schema:
                    row_dict[field] = row[table_schema.index(field)]
                # print the row
                print_row(row_dict, table_schema, format_str, FIELD_PRINT_LEN)

        # clear the Temp directory
        for file in os.listdir(f"{BASE_DIR}/Temp"):
            os.remove(f"{BASE_DIR}/Temp/{file}")
        

    def _merge_sorted_chunks(self, field, schema, order_method, pass_num) -> str:
        # get the path of the temp directory
        temp_path = f"{BASE_DIR}/Temp"
        # get the index of the field
        field_index = schema.index(field)
        # find the max chunk number under the temp_path
        max_chunk_num = -1
        for file in os.listdir(temp_path):
            # ignore the files not in current pass
            if file.endswith(".csv") and file.split(".")[0].split("_")[3] == str(pass_num):
                chunk_num = int(file.split("_")[1])
                if chunk_num > max_chunk_num:
                    max_chunk_num = chunk_num

        if max_chunk_num == 0 or max_chunk_num == -1:
            # no need to merge
            return f"{temp_path}/chunk_0_pass_{pass_num}.csv"

        next_chunk_num = 0 # the chunk number of the merged file in the next pass
        start_chunk_num = 0 # the starting chunk of current merge group
        end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1)
        while start_chunk_num <= max_chunk_num:
            output_file = f"{temp_path}/chunk_{next_chunk_num}_pass_{pass_num + 1}.csv"
            reader_dict = {} # key: chunk_num, value: csv_reader
            loaded_rows = PriorityQueue() # pq of tuples (key, (row, chunk_num))
            # open the csv readers for all the chunks in the current merge group
            opened_files = []
            for chunk_num in range(start_chunk_num, end_chunk_num):
                chunk_file_name = f"{temp_path}/chunk_{chunk_num}_pass_{pass_num}.csv"
                opened_file = open(chunk_file_name, "r")
                opened_files.append(opened_file)
                reader_dict[chunk_num] = csv.reader(opened_file)
            # load the first row from each chunk into the heap
            for chunk_num in range(start_chunk_num, end_chunk_num):
                csv_reader = reader_dict[chunk_num]
                row = next(csv_reader, None)
                if row is not None:
                    row_element = RowElement(chunk_num, row, field_index, order_method)
                    loaded_rows.put(row_element)
            # output until the heap is empty
            while not loaded_rows.empty():
                row_element = loaded_rows.get()
                row = row_element.row
                chunk_num = row_element.chunk_num
                # write the row to the output file
                with open(output_file, "a") as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerow(row)
                # load the next row from the same chunk
                next_row = next(reader_dict[chunk_num], None)
                if next_row is not None:
                    row_element = RowElement(chunk_num, next_row, field_index, order_method)
                    loaded_rows.put(row_element)
            # close all open files
            for opened_file in opened_files:
                opened_file.close()
            # update the start/end_chunk_num and next_chunk_num
            start_chunk_num += CHUNK_SIZE
            end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1)
            next_chunk_num += 1

        # proceed to the next pass
        return self._merge_sorted_chunks(field, schema, order_method, pass_num + 1)


        


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
            csv_reader = csv.reader(f)
            schema = next(csv_reader)
        return tuple(schema)
    
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
            csv_reader = csv.reader(f)
            table_schema = next(csv_reader)
            # write the schema to the schema.txt
            with open(f"{table_storage_path}/schema.txt", "w") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(table_schema)
        
        # load the rest of the data to the storage using _insert_row
        with open(csv_file_path, "r") as f:
            csv_reader = csv.reader(f)
            next(csv_reader) # skip the first line
            for row in csv_reader:
                self._insert_row(table_name, row)


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
                csv_writer = csv.writer(f)
                csv_writer.writerow(row)
        else:
            # check if the last chunk is full
            with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "r") as f:
                csv_reader = csv.reader(f)
                rows = list(csv_reader)
                if len(rows) < CHUNK_SIZE:
                    # append to the last chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "a") as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow(row)
                else:
                    # create a new chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num + 1}.csv", "w") as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow(row)

