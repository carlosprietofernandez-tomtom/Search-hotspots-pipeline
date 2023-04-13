from flask import Flask
from flask.templating import render_template
import os

app = Flask(__name__, static_url_path="/static")

folders = os.listdir("apps")
folders.sort()


@app.route("/")
def index():
    return render_template("index.html", folders=folders, l=len(folders))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=80)
