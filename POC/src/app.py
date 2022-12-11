from flask import Flask, request, Response, render_template, flash, redirect, url_for, session
import os

from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)
CORS(app)
app.secret_key = 'ThisIsaSecret'
app.config['SESSION_TYPE'] = 'filesystem'

####################################

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['0.0.0.0'],port=9042, auth_provider=auth_provider)
session = cluster.connect('tutorialspoint',wait_for_all_pools=True)
try : 
    rows = session.execute("CREATE KEYSPACE tutorialspoint WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};")
except Exception :
    pass 
    
session.execute('USE tutorialspoint')
#####################################

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/analyze", methods=["POST"])
def analyze(): 
    
    # check if the post request has the file part
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    file = request.files['file']

def init_db(): 
    try : 
        session.execute("CREATE TABLE tutorialspoint.users ( id int PRIMARY KEY,firstname text,lastname text );")
        session.execute("CREATE TABLE tutorialspoint.books ( id int PRIMARY KEY, title text, author text );")
        session.execute("CREATE TABLE tutorialspoint.users_books ( user_id int, book_id int, PRIMARY KEY (user_id, book_id));")
    except : 
        pass
    #Insert values
    session.execute("INSERT INTO tutorialspoint.users (id, firstname, lastname) VALUES (1, 'John', 'Doe');")
    session.execute("INSERT INTO tutorialspoint.users (id, firstname, lastname) VALUES (2, 'Jason', 'Momoha')")
    session.execute("INSERT INTO tutorialspoint.users (id, firstname, lastname) VALUES (3, 'Johnny', 'Depp');")

    session.execute("INSERT INTO tutorialspoint.books (id, title , author ) VALUES (1, 'Dune', 'Frank Herbert')")
    session.execute("INSERT INTO tutorialspoint.books (id, title , author ) VALUES (2, 'Star Wars', 'Georges Lucas')")
    session.execute("INSERT INTO tutorialspoint.books (id, title , author ) VALUES (3, 'Harry Potter', 'J.K Rowling')")
    
    session.execute("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (1, 2)")
    session.execute("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (2, 2)")
    session.execute("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (3, 2)")
    session.execute("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (2, 1)")
    session.execute("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (3, 3)")

init_db()

if __name__ == '__main__':
    app.debug = True
    app.run()