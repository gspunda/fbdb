import mysql.connector
import requests
import re
import sys
from bs4 import BeautifulSoup

#Checks if the record scraped from the url is present in the database.
def check_record(round, tname):
    db = db_connect()
    c = db.cursor()
    c.execute("USE fbdb")
    statement = f"SELECT COUNT(*) FROM {tname} WHERE round = %s"
    c.execute(statement, (round,))
    result = c.fetchone()
    if result[0] > 0: c.close(); db.close(); return True;
    c.close()
    db.close()
    return False

#Inserts details into database after checkinf if the the records already exist.
def insert_match_info(score, round, home, away, _date):
    tname = sys.argv[2]
    check = check_record(round, tname)
    db = db_connect()
    c = db.cursor()
    if check == False: #If record doesn't exist, it inserts it.
        score_h, score_a = map(int, score.split("-"))
        statement = f"INSERT INTO {tname} (date, round, team_home, team_away, home_score, away_score) VALUES (%s, %s, %s, %s, %s, %s)"
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


#Exracts url from database, which leads to the teams matches.
def fetch_url():
    tname = sys.argv[2]
    db = db_connect()
    c = db.cursor()
    c.execute("USE fbdb")
    statement = "SELECT url FROM teams WHERE name = %s"
    c.execute(statement, (tname, ))
    url = str(c.fetchall())
    url = url.split("'")[1::2]
    return url[0]

#Extracts scores of the team provided as an argument.
#Also provides links to every match for further scraping.
def fetch_match():
    url = fetch_url()
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

#Is called when script is executed with the -at --addteam flag.
def add_team():
        table_name = sys.argv[2]
        db = db_connect()
        c = db.cursor()
        mysql_statements = [
        "USE fbdb",
        "INSERT INTO teams (name, url) VALUES (%s, %s)",
        "CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY, date VARCHAR(255), round VARCHAR(255), team_home VARCHAR(255), team_away VARCHAR(255), home_score INT, away_score INT)"]
        create_table_query = mysql_statements[2].format(table_name)
        c.execute(mysql_statements[0])
        c.execute(mysql_statements[1], (sys.argv[2], sys.argv[3]))
        create_table_query = mysql_statements[2].format(table_name)
        c.execute(create_table_query)
        db.commit()
        c.close()
        db.close()

#Displays teams which are present in the database.
def show_teams():
        table_name = "teams"
        display_table(table_name)

#Displays all the matches of the chosen team.
def show_matches():
        table_name = sys.argv[2]
        display_table(table_name)

#Checks if the number of arguments.
#Returns False is it's lower then provided number.
def check_argvs(l):
    if len(sys.argv) < l: return False;
    return True

#Deletes the team provided as an argument.
def delete_team():
    tname = sys.argv[2]
    db = db_connect()
    c = db.cursor()
    mysql_statements = [
        "USE fbdb",
        "DELETE FROM teams WHERE name = %s",
        f"DROP TABLE IF EXISTS {tname}"]
    c.execute(mysql_statements[0])
    c.execute(mysql_statements[1], (tname,))
    c.execute(mysql_statements[2])
    db.commit()
    c.close()
    db.close()

#Displays basic help.
def display_help():
    print("Start a program with -c or--create parameter in order to create the new Database for the program. If it doesn't already exists\n")

    print("Start a program with -st or --showteams parameter in order to display the teams added to the program.\n")

    print("Start a program with -at or --addteams parameter and the name of the team as a second parameter and url to the team's matches page from 90minut.pl, in order to add a team to the database.")
    print("IMPORTANT: Validity of the url is crucial for the program to properly collect the data. Below is the example url for the matches: \nhttp://www.90minut.pl/mecze_druzyna.php?id=26775&id_sezon=105")
    print("Also make sure to put the url in quotation marks.\n")


    print("Start a program with -u or --update parameter and name of the team, which is already in the database, in order to collect matches data from the 90minut.pl.\n")

    print("Start a program with -sm or --showmatches parameter and name of the team, which is already in the database, in order to show team's played matches.\n")

    print("Start a program with -dt or --deleteteam parameter and name of the team, which is already in the database, in order to dlete this team from the database.\n")

#Main function, acts on the input given when executing the script.
def main():
    message = "No arguments! Please execute the script with the -h or --help parameter for help!"
    if check_argvs(2) == False: print(message); return;
    if sys.argv[1] == ("-h" or "--help"): display_help(); return;
    if sys.argv[1] == ("-c" or "--create"): db_create(); return;
    if sys.argv[1] == ("-st" or "--showteams"): show_teams(); return;
    if sys.argv[1] == ("-at" or "--addteam") and (check_argvs(4) == True):
        add_team(); return;
    if sys.argv[1] == ("-u" or "--update") and (check_argvs(3) == True):
        fetch_match(); return;
    if sys.argv[1] == ("-sm" or "--showmatches") and (check_argvs(3) == True):
        show_matches(); return;
    if sys.argv[1] == ("-dt" or "--deleteteam") and (check_argvs(3) == True):
        delete_team(); return;

if __name__ == '__main__':
    # fetch_match(http://www.90minut.pl/mecze_druzyna.php?id=26775&id_sezon=105)
    # main()
    main()
