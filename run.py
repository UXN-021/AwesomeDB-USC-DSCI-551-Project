from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from Engine.relational import Relational  

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

@app.route('/update_data', methods=['POST'])
def update_data():
    table_name = request.form['table_name']
    condition = request.form['condition']
    data = request.form['data'].split(',')
    result = db_engine.update_data(table_name, condition, data)
    return jsonify({'result': result})

if __name__ == '__main__':
    app.run(debug=True)