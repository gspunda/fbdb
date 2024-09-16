import mysql.connector
import requests
import re
import sys
from bs4 import BeautifulSoup

#Checks if the record scraped from the url is present in the database.
def check_record(round):
    db = db_connect()
    c = db.cursor()
    c.execute("USE fbdb")
    statement = "SELECT COUNT(*) FROM matches WHERE round = %s"
    c.execute(statement, (round,))
    result = c.fetchone()
    if result[0] > 0: c.close(); db.close(); return True;
    c.close()
    db.close()
    return False

#Inserts details into database after checkinf if the the records already exist.
def insert_match_info(score, round, home, away, _date):
    check = check_record(round)
    db = db_connect()
    c = db.cursor()
    if check == False: #If record doesn't exist, it inserts it.
        score_h, score_a = map(int, score.split("-"))
        statement = "INSERT INTO matches (date, round, team_home, team_away, home_score, away_score) VALUES (%s, %s, %s, %s, %s, %s)"
        c.execute("USE fbdb")
        c.execute(statement, (_date, round, home, away, score_h, score_a))
        db.commit()
        c.close()
        db.close()

#Extracts data from the match page.
def extract_match_info(a_class_mecze, td_tags):
    tag_iterator = 9 #Starts with the 10th element of the td tags, which is the     first  item to extract.
    for i in a_class_mecze:
        str(i)
        match_link = ("http://www.90minut.pl" + (re.findall('"([^"]*)"', str(i)))[1]) #extracts link to the match for future features.
        match_score = i.string
        match_id  = match_link.split("=",1)[1] #match_id for future features.
        match_round =((td_tags[tag_iterator].string).split(",",1)[1]).replace(
        " ", "")
        match_date = td_tags[tag_iterator].previous_element.string
        home_team = td_tags[tag_iterator].next_element.next_element.string
        away_team = match_score.next_element.string
        insert_match_info(
        match_score, match_round, home_team, away_team, match_date)
        tag_iterator += 5 #Incerements to the next table row in the url.

#Extracts scores of the team provided in the url
#Also provides links to every match for further scraping.
def fetch_match(url):
    page = requests.get(url)
    doc = BeautifulSoup(page.text, "html.parser")
    a_class_mecze = doc.find_all("a", class_ = "mecze2")
    td_tags = doc.find_all("td", string=True)
    tr_tags = doc.find_all("tr")
    extract_match_info(a_class_mecze, td_tags)

def display_table(table):
    db = db_connect()
    c = db.cursor()
    c.execute("USE fbdb")
    statement = f"SELECT * FROM {table}"
    c.execute(statement)
    results = c.fetchall()
    for row in results:
        print(row)

    c.close()
    db.close()

#Function for handling database connection.
def db_connect():
    try:
        db = mysql.connector.connect(
        host="localhost",
        user="fbdb",
        password="password")
        database="fbdb"
    except mysql.connector.Error as e:
        print("Error reading data from MySQL table: ", e)
    return db

#Creates tables for the database.
def create_teams_table():
        db = db_connect()
        c = db.cursor()
        mysql_statements = [
        "USE fbdb",
        "CREATE TABLE IF NOT EXISTS teams (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), url VARCHAR(255))"]
        for statement in mysql_statements: c.execute(statement)
        c.close()
        db.close()

#Creates database and tables if they do not already exist.
def db_create():
    try:
        db = mysql.connector.connect(
        host="localhost",
        user="fbdb",
        password="password")
        c = db.cursor()
        c.execute("CREATE DATABASE IF NOT EXISTS fbdb")
        c.close()
        db.close()
        create_teams_table()
    except mysql.connector.Error as e:
        print("Error connecting to the MySQL: ", e)

def update_db():
    pass

#Is called when script is executed with the -at --addteam flag.
def add_team():
        table_name = sys.argv[2]

        db = db_connect()
        c = db.cursor()
        mysql_statements = [
        "USE fbdb",
        "INSERT INTO teams (name, url) VALUES (%s, %s)",
        "CREATE TABLE IF NOT EXISTS '{}' (id INT AUTO_INCREMENT PRIMARY KEY, date VARCHAR(255), round VARCHAR(255), team_home VARCHAR(255), team_away VARCHAR(255), home_score INT, away_score INT)"]
        create_table_query = mysql_statements[2].format(table_name)
        c.execute(mysql_statements[0])
        c.execute(mysql_statements[1], (sys.argv[2], sys.argv[3]))
        create_table_query = mysql_statements[2].format(table_name)
        c.execute(create_table_query)
        db.commit()
        c.close()
        db.close()

#Displays help with explanation of the stript's functions.
def display_help():
    print("Help!")

#Displays teams which are present in the database.
def show_teams():
    print("show teams!")

#Displays all the matches of the chosen team.
def show_matches():
    print("show matches!")

#Checks if the number of arguments.
#Returns False is it's lower then provided number.
def check_argvs(l):
    if len(sys.argv) < l: return False;
    return True

#Deletes the team provided as an argument.
def delete_team():
    pass

#Main function, acts on the input given when executing the script.
def main():
    message = "No arguments! Please execute the script with the -h or --help argument for help."
    if check_argvs(2) == False: print(message)
    if sys.argv[1] == ("-h" or "--help"): display_help(); return;
    if sys.argv[1] == ("-c" or "--create"): db_create(); return;
    if sys.argv[1] == ("-st" or "--showteams"): show_teams(); return;
    if sys.argv[1] == ("-at" or "--addteam") and (check_argvs(4) == True):
        add_team(); return;
    if sys.argv[1] == ("-u" or "--update") and (check_argvs(3) == True):
        update_db(); return;
    if sys.argv[1] == ("-sm" or "--showmatches") and (check_argvs(3) == True):
        show_matches(); return;
    if sys.argv[1] == ("-dt" or "--deleteteam") and (check_argvs(3) == True):
        show_matches(); return;




if __name__ == '__main__':
    # fetch_match("http://www.90minut.pl/mecze_druzyna.php?id=26775&id_sezon=105")
    main()
