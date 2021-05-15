from flask import Flask,render_template,request,Response,jsonify
import redis
import json


app=Flask(__name__)


@app.route('/sendjson', methods=['POST'])
def sendjson():
    print(request.get_json())
    print("###################")
    # 接受前端发来的数据
    data = json.loads(request.get_data("data"))


    # lesson: "Operation System"
    # score: 100
    lesson = data["lesson"]
    score = data["score"]
    print(lesson,score)
    # 自己在本地组装成Json格式,用到了flask的jsonify方法
    info = dict()
    info['name'] = "pengshuang"
    info['lesson'] = lesson
    info['score'] = score
    print(jsonify(info))
    return jsonify(info)

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=8085,debug=True)
