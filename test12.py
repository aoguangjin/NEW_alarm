import json
from flask import Flask
from flask import request
from flask import redirect
from flask import jsonify
app = Flask(__name__)

@app.route('/' , methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        return json.dumps(dict1["data"])
    else:
        return '<h1>只接受post请求！</h1>'

@app.route('/user/<name>')
def user(name):
    return'<h1>hello, %s</h1>' % name

if __name__ =='__main__':
    app.run(debug=True)