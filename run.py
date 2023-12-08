from flask import Flask, render_template, request, send_from_directory
from flask_cors import CORS
from Engine.nosql import NoSQL
from Engine.relational import Relational

from config import BASE_DIR

app = Flask(__name__)
app.config["RESULT_DIR"] = f"{BASE_DIR}/Results"
app.config["RELATIONAL_ENGINE"] = Relational()
app.config["NOSQL_ENGINE"] = NoSQL()

@app.route('/')
def index():
    return send_from_directory("static", "index.html")

@app.route('/projection', methods=['POST'])
def show_data():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    fields = data.get('fields').split(',')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].projection(table_name, fields, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].projection(table_name, fields, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/filtering', methods=['POST'])
def filtering():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    fields = data.get('fields').split(',')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].filtering(table_name, fields, condition, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].filtering(table_name, fields, condition, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/updating', methods=['POST'])
def updating():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    data_val = data.get('data').split(',')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].update_data(table_name, condition, data_val, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].update_data(table_name, condition, data_val, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/deletion', methods=['POST'])
def deletion():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].delete_data(table_name, condition, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].delete_data(table_name, condition, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/insertion', methods=['POST'])
def insertion():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    data_val = data.get('data').split(',')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].insert_data(table_name, data_val, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].insert_data(table_name, data_val, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/sorting', methods=['POST'])
def sorting():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    field = data.get('field')
    method = data.get('method')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].order(table_name, field, method, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].order(table_name, field, method, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

if __name__ == "__main__":
	app.run()