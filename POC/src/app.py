from flask import Flask, request, Response, render_template, flash, redirect, url_for, session
import os

from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
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
    rows = session.execute("ALTER KEYSPACE system_auth WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    
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
    status = os.popen("docker exec bitnami-docker-cassandra-cassandra1-1 nodetool status tutorialspoint").read()
    
    regex = r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b"
    match = re.search(regex,status , re.MULTILINE).group()
    ip1 = match.split('.')[0]
    ip2 = match.split('.')[1]
    ip3 = match.split('.')[2]
    os.popen(f'docker exec bitnami-docker-cassandra-cassandra1-1 nodetool assassinate {ip1}.{ip2}.{ip3}.{int(node) + 1}')
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
    query = SimpleStatement("INSERT INTO tutorialspoint.users (id, firstname, lastname) VALUES (%s, %s, %s);", consistency_level=ConsistencyLevel.QUORUM)
    session.execute(query, (1, 'John', 'Doe'))
    session.execute(query, (2, 'Jason', 'Momoha'))
    session.execute(query, (3, 'Johnny', 'Depp'))

    query = SimpleStatement("INSERT INTO tutorialspoint.books (id, title , author ) VALUES (%s, %s, %s);", consistency_level=ConsistencyLevel.QUORUM)
    session.execute(query, (1, 'Dune', 'Frank Herbert'))
    session.execute(query, (2, 'Star Wars', 'Georges Lucas'))
    session.execute(query, (3, 'Harry Potter', 'J.K Rowling'))

    
    query = SimpleStatement("INSERT INTO tutorialspoint.users_books (user_id, book_id) VALUES (%s, %s);", consistency_level=ConsistencyLevel.QUORUM)

    session.execute(query, (2, 2))
    session.execute(query, (1, 3))
    session.execute(query, (1, 2))
    session.execute(query, (3, 1))
    session.execute(query, (3, 2))
    session.execute(query, (2, 1))

init_db()

if __name__ == '__main__':
    app.debug = True
    app.run()