FIFA 23 ETL Pipeline and Dashboard: A User Manual

Hello! Welcome to the FIFA 23 Data Project.

This guide will walk you through every step to run this project on your own computer. This project takes a large data file about FIFA 23 players, uses special scripts to clean and organize it, saves it to a professional database, and finally, lets you explore the data with a cool, interactive dashboard.

No technical experience is needed! Just follow the steps below carefully.

Part 1: Before You Start: Getting Your Computer Ready

Before we can run the project, we need to make sure your computer has the necessary tools. Think of this as gathering your ingredients before cooking. These are one-time installations.

We will be using the Terminal (also called the command line) to run these commands. You can find it by searching for "Terminal" in your applications.

Tool 1: Git
What it is: A tool to copy the project's code from the internet (from a website called GitHub).
Bash
sudo apt install git


Tool 2: Python
The programming language the entire project is written in.
Bash:
sudo apt install python3 python3-pip python3-venv


Tool 3: Java (Version 17)
 A requirement for PySpark, the powerful tool we use to clean the data.
Bash
sudo apt install openjdk-17-jdk

Tool 4: PostgreSQL (Our Database)
 A professional, open-source database where we will store all our clean player data.
Bash
sudo apt install postgresql postgresql-contrib

Tool 5: A Python Virtual Environment (for PySpark)
A clean, separate "sandbox" for our Python tools so they don't interfere with your system. We will follow your course's standard of having a central one.
This guide assumes you have already created a PySpark virtual environment at ~/Workspace/pyspark/venv/ as shown in your setup manuals.

Part 2: Setting Up the Database (One-Time Setup)
Now we need to prepare our new PostgreSQL database by creating a user and a special database for our FIFA data.

Step 2.1: Start the Database Service
Your database runs as a service in the background. Let's make sure it's running.
Bash
sudo systemctl start postgresql
sudo systemctl enable postgresql

Step 2.2: Create Our Database User and a New Database
We need to log in as the main database administrator (postgres) to do this.
Log in as the postgres user:
Bash
sudo -i -u postgres
(Your terminal prompt will change to postgres@...)

Start the PostgreSQL command line:
Bash
psql
(Your prompt will now be postgres=#)

Copy and paste these commands one by one, pressing Enter after each one:
SQL
CREATE USER fifauser WITH PASSWORD 'fifapassword';
CREATE DATABASE fifadb OWNER fifauser;
\q
Log out of the postgres user session:
Bash
exit
(You should be back to your normal prashuna@... prompt).

Step 2.3: Allow Password Logins
By default, PostgreSQL is very strict. We need to tell it to allow our new fifauser to log in with its password.
Open the configuration file with a simple text editor:
code
Bash
sudo nano /etc/postgresql/16/main/pg_hba.conf

Use the arrow keys to scroll down and find the lines that start with local and host.
Change the last word on those lines (peer or scram-sha-256) to md5. It should look like this:


local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
Save and exit: Press Ctrl + X, then type Y, then press Enter.

Restart the database to apply the changes:

Bash
sudo systemctl restart postgresql
The database is now fully prepared!

Part 3: Running the Project
Now for the exciting part! Let's get the code and run our data pipeline.

Step 3.1: Get the Project Code

Bash
# Go to a folder where you like to keep projects, like Workspace
cd ~/Workspace/

# Copy the project from GitHub (replace the URL with your own)
git clone (https://github.com/PrashunaBuddhacharya/fifa-etl-pipeline)

# Go into the new project folder
cd fifa-etl-project

Step 3.2: Get the Required Files
FIFA Data: Go to the Kaggle FIFA Dataset page, download the zip file, rename it to fifa1.zip, and move it into the data/ folder inside your new project.
Database Connector: Go to the PostgreSQL JDBC Driver page, download the latest .jar file, and move it directly into your main fifa-etl-project folder.

Step 3.3: Run the ETL Pipeline
This is where the magic happens. We will run the three scripts in order.
Activate your Python sandbox:

Bash
source ~/Workspace/pyspark/venv/bin/activate
(Your terminal prompt will now start with (venv))

Run the EXTRACT script: This unzips the data.

Bash
python3 extract/execute.py extracted_data
Run the TRANSFORM script: This cleans the data.
(First, check the exact name of the unzipped file with ls extracted_data)

Bash
python3 transform/execute.py extracted_data/players_22.csv transformed_data
Run the LOAD script: This saves the clean data to your database.

Bash
python3 load/execute.py transformed_data
Your data is now processed and stored!

Part 4: View the Results in the Streamlit Dashboard
Now that the data is in the database, you can launch the interactive web dashboard.
Make sure you are still in the fifa-etl-project folder and your (venv) is active.
Run the Streamlit application:
code
Bash
streamlit run app.py
Your web browser should automatically open a new tab to http://localhost:8501.
You can now see and interact with your dashboard, exploring all the FIFA player data you just processed!
Congratulations! You have successfully run the entire project from start to finish.
