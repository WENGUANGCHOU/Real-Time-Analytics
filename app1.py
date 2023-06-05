
from flask import Flask

# Create a flask
app = Flask(__name__)

# Create an API end point
@app.route('/hello', methods=['GET'])
def say_hello():
    return "<b>Hello</b>"

if __name__ == '__main__':
    app.run()
