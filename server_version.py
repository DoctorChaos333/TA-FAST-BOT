from flask import Flask, request
from flask_restful import Api, Resource
import json

app = Flask(__name__)
api = Api(app)

class Main(Resource):
    def post(self):
        data = request.get_json()
        with open('base.json', 'r', encoding='utf-8') as file:
            base = eval(file.read())
        key = data['key']
        current_machine_id = data['current_machine_id']
        if key in base:
            if base[key] == '' or base[key] == current_machine_id:
                base[key] = current_machine_id
                with open('base.json', 'w', encoding='utf-8') as file:
                    json.dump(base, fp=file, indent=4)
                response = {'response': True}
            else:
                response = {'response': False}
        else:
            response = {'response': False}
        return response

api.add_resource(Main, '/api')

if __name__ == '__main__':
    app.run(debug=True, port=1337, host='185.195.24.244')