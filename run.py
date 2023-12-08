from flask import Flask, render_template, request, jsonify, Response
from contextlib import contextmanager
from flask_cors import CORS
from Engine.relational import Relational  
from io import TextIOBase
from io import StringIO
import io
import sys



app = Flask(__name__)
CORS(app)  


relational_db = Relational()



@app.route('/')
def index():
    return render_template('page.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        file = request.files['file']
        if file:
            file.save(f'ToBeLoaded/{file.filename}')  
            result = relational_db.load_data(file.filename)  
            return jsonify({'success': result})
        else:
            return jsonify({'error': 'No file provided'})
    except Exception as e:
        return jsonify({'error': str(e)})

   

@app.route('/create_table', methods=['POST'])
def create_table():
    table_name = request.form['table_name']
    fields = request.form['fields'].split(',')
    result = db_engine.create_table(table_name, fields)
    return jsonify({'result': result})

@app.route('/insert', methods=['POST'])
def insert_data():
    try:
        table_name = request.form['table_name']
        data = request.form['data']
        result = relational_db.insert_data(table_name, data.split(','))
        return jsonify({'success': result})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/delete_data', methods=['POST'])
def delete_data():
    table_name = request.form['table_name']
    condition = request.form['condition']
    result = db_engine.delete_data(table_name, condition)
    return jsonify({'result': result})

@app.route('/update', methods=['POST'])
def update_data():
    try:
        table_name = request.form['tableName']
        condition = request.form['condition']
        updated_data = request.form['updatedData']
        result = relational_db.update_data(table_name, condition, updated_data.split(','))
        return jsonify({'success': result})
    except Exception as e:
        return jsonify({'error': str(e)})
    
# @app.route('/projection', methods=['POST'])
# def project_fields():
#     try:
#         table_name = request.form.get('table_name')
#         fields = request.form.get('fields').split(',')

    
#         result_content = relational_db.projection(table_name, fields)

        
#         return jsonify({'success': True, 'content': result_content})

#     except Exception as e:
#         return jsonify({'success': False, 'error': str(e)})

@app.route('/projection', methods=['POST'])
def project_fields():
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        fields = data.get('fields').split(',')

        result_content = relational_db.projection(table_name, fields)

        return jsonify({'success': True, 'content': result_content})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/filter', methods=['POST'])
def apply_filter():
    try:
        table_name = request.form.get('table_name')
        fields = request.form.get('fields').split(',')
        condition = request.form.get('condition')

        output = relational_db.filtering(table_name, fields, condition)

        return jsonify({'success': True, 'output': output})

    except Exception as e:
        return jsonify({'error': str(e)})
    

@app.route('/aggregate', methods=['POST'])
def aggregate():
    try:
        data = request.form

        table_name = data.get('table_name')
        aggregate_method = data.get('aggregate_method')
        aggregate_field = data.get('aggregate_field')
        group_by_field = data.get('group_by_field')

        # Call the aggregate method of your Relational class
        result = relational_db.aggregate(
            table_name,
            aggregate_method,
            aggregate_field,
            group_by_field
        )

        return jsonify({'success': True, 'result': result})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    

@app.route('/order', methods=['POST'])
def order():
    data = request.get_json()

    table_name = data['table_name']
    order_field = data['field']
    order_direction = data['order_method']

    # Call the order method from the Relational instance
    success = relational_db.order(table_name, order_field, order_direction)

    if success:
        return jsonify({'success': True, 'message': 'Order successful'})
    else:
        return jsonify({'success': False, 'error': 'Order failed'})
    
@app.route('/join', methods=['POST'])
def join_tables():
    try:
        # Get data from the request
        table1 = request.form.get('table1')
        table2 = request.form.get('table2')
        condition = request.form.get('condition')

        # Perform the join operation
        result = relational_engine.join(table1, table2, condition)

        return jsonify({'success': True, 'message': 'Join operation successful'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

       

if __name__ == '__main__':
    app.run(debug=True)