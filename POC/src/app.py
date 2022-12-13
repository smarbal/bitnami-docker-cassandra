from flask import Flask, request, Response, render_template, flash, redirect, url_for, session
import os

from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import re 

app = Flask(__name__)
CORS(app)
app.secret_key = 'ThisIsaSecret'
app.config['SESSION_TYPE'] = 'filesystem'

####################################

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['0.0.0.0'],port=9042, auth_provider=auth_provider)
session = cluster.connect()
try : 
    rows = session.execute("CREATE KEYSPACE tutorialspoint WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};")
except Exception :
    pass 

session.execute('USE tutorialspoint')
#####################################

@app.route("/")
def index():
    users = session.execute("SELECT * FROM tutorialspoint.users;")
    user_books = session.execute("SELECT * FROM tutorialspoint.users_books;")
    books = session.execute("SELECT * FROM tutorialspoint.books;")
    regex = r"\b(?<!\.)(?!0+(?:\.0+)?%)(?:\d|[1-9]\d|100)(?:(?<!101)\.\d+)?%"
    status = os.popen("docker exec bitnami-docker-cassandra-cassandra1-1 nodetool status tutorialspoint").read()
    matches = re.finditer(regex,status , re.MULTILINE)
    result =[]
    for match in matches : 
        result.append(match.group())
    #status = re.match("\b(?<!\.)(?!0+(?:\.0+)?%)(?:\d|[1-9]\d|100)(?:(?<!100)\.\d+)?%", status)
    return render_template('index.html', users= users, user_books = user_books, books = books, status = result)





@app.route("/kill", methods=["GET"])
def analyze(): 
    
    node = request.args.get('node')
    os.popen(f'docker exec bitnami-docker-cassandra-cassandra1-1 nodetool assassinate 172.28.0.{int(node) + 1}')
    #os.popen(f'docker kill bitnami-docker-cassandra-cassandra{node}-1')

    return 'Ok', 200





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