from abc import abstractmethod
import re


class BaseEngine():
    command_dict = {
        "show_all": r'show tables;',
        "create_table": r'create table (.*?);',
        "insert_data": r'insert into (.*?) with data (.*?);',
        "delete_data": r'delete from (.*?) where (.*?);',
        "update_data": r'update in (.*?) where (.*?) and set (.*?);',
        "projection": r'show column (.*?) from (.*?);',
        "filtering": r'show data (.*?) from (.*?) where (.*?);',
        "join": r'join (.*?) and (.*?) on (.*?);',
        "aggregate": r'find (.*?) from (.*?) group by (.*?);',
        "order": r'sort data in (.*?) by (.*?) (.*?);',
        "exit": r'exit',
        "load_data": r'load data from (.*?);',
    }

    def parse_and_execute(self, input_str):
        if re.match(self.command_dict['exit'], input_str):
            return False
        elif re.match(self.command_dict['load_data'], input_str):
            # load data
            # example: load data from xxx.csv
            file_name = re.match(self.command_dict['load_data'], input_str).group(1)
            print(file_name)
            self.load_data(file_name)
        elif re.match(self.command_dict['show_all'], input_str):
            # show all tables
            # example: show tables
            self.show_tables()
        elif re.match(self.command_dict['create_table'], input_str):
            # create table
            # example: create table table_name(field1,field2,field3)
            kwargs = re.match(self.command_dict['create_table'], input_str).group(1)
            table_schema = re.match(r'(.*?)\((.*?)\)', kwargs)
            database_name = table_schema.group(1)  # xxx.csv
            print(database_name)
            field_str = table_schema.group(2)  # "xxx,xxx,xxx"
            self.create_table(database_name, field_str)
        elif re.match(self.command_dict['insert_data'], input_str):
            # insert data
            # example: insert into table_name with data id=4,address=east42
            kwargs = re.match(self.command_dict['insert_data'], input_str)
            table_name = kwargs.group(1)
            data = kwargs.group(2)
            self.insert_data(table_name, data)
        elif re.match(self.command_dict['delete_data'], input_str):
            # delete data
            # example: delete from table_name where id=4
            kwargs = re.match(self.command_dict['delete_data'], input_str)
            table_name = kwargs.group(1)
            condition = kwargs.group(2)
            self.delete_data(table_name, condition)
        elif re.match(self.command_dict['update_data'], input_str):
            # update data
            # example: update in table_name where id=4 and set address=east42,id=5
            kwargs = re.match(self.command_dict['update_data'], input_str)
            table_name = kwargs.group(1)
            condition = kwargs.group(2)
            data = kwargs.group(3)
            self.update_data(table_name, condition, data)
        elif re.match(self.command_dict['projection'], input_str):
            # projection
            # example: show column id,name from table_name
            kwargs = re.match(self.command_dict['projection'], input_str)
            fields = kwargs.group(1)
            table_name = kwargs.group(2)
            self.projection(table_name, fields)
        elif re.match(self.command_dict['filtering'], input_str):
            # filtering
            # example: show data id,name from table_name where id=4
            kwargs = re.match(self.command_dict['filtering'], input_str)
            fields = kwargs.group(1)
            table_name = kwargs.group(2)
            condition = kwargs.group(3)
            self.filtering(table_name, fields, condition)
        elif re.match(self.command_dict['order'], input_str):
            # order
            # example: sort data in table_name by id desc
            kwargs = re.match(self.command_dict['order'], input_str)
            table_name = kwargs.group(1)
            field = kwargs.group(2)
            order_method = kwargs.group(3)
            # check if order_method is valid
            if order_method not in ['asc', 'desc']:
                print("order method must be asc or desc")
                return True
            self.order(table_name, field, order_method)
        elif re.match(self.command_dict['join'], input_str):
            # join
            # example: join table1 and table2 on table1.id=table2.id
            kwargs = re.match(self.command_dict['join'], input_str)
            table1 = kwargs.group(1)
            table2 = kwargs.group(2)
            condition = kwargs.group(3)
            self.join(table1, table2, condition)

        return True



    @abstractmethod
    def show_tables(self):
        pass

    @abstractmethod
    def create_table(self, database_name, fields):
        pass

    @abstractmethod
    def insert_data(self, table_name, data):
        pass

    @abstractmethod
    def delete_data(self, table_name, condition):
        pass

    @abstractmethod
    def update_data(self, table_name, condition, data):
        pass

    @abstractmethod
    def projection(self, table_name, fields):
        pass

    @abstractmethod
    def filtering(self, table_name, fields, condition):
        pass

    @abstractmethod
    def join(self, left, right, condition):
        pass

    @abstractmethod
    def group():
        pass

    @abstractmethod
    def aggregate():
        pass

    @abstractmethod
    def order(self, table_name, field, order_method):
        pass

    @abstractmethod
    def load_data(self, file_name):
        pass

