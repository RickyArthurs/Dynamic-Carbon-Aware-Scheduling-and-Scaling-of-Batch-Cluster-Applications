import sqlite3
import pandas as pd

# example inputs
# 1 15 passengers.py 1,9 4
# 2  passengers.py 1,9 12
# 3 30 passengers.py 1,9 24


def main():
    # unique identifier for each job initialized
    id = 0
    # connection established to database
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    # reset the table each time the program is ran
    cursor.execute("DROP TABLE IF EXISTS `jobs`;")
    # create table statement with corresponding data types for each job
    create_statement = """CREATE TABLE jobs (
    submissionTime,
    id int,
    runtime int,
    filename varchar(255),
    args varchar(255),
    deadline int
    );"""
    cursor.execute(create_statement)

    # loop such that program continually prompts user for submissions
    while True:
        try:
            id, runtime, filename, args, noHours = input(
                "Ready for job submission, enter the following parameters with spaces inbetween: (id runtime filename args deadline) \n"
            ).split(" ")
        except ValueError as ve:
            print("Incorrect input detected: \n")
            print(ve)
            continue

        # deadline calculated from number of hours allowed for shifting
        try:
            deadLine = pd.Timestamp.today() + pd.Timedelta(hours=int(noHours))
        except ValueError as ve:
            print("Format inputted for deadline incorrect i.e. not a number")
            print(ve)
            continue

        # insert values into table, validating their data types if insertion successful
        try:
            cursor.execute(
                """INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    pd.Timestamp.today().isoformat(),
                    id,
                    runtime,
                    filename,
                    args,
                    deadLine.isoformat(),
                ),
            )
            conn.commit()
        except sqlite3.Error as error:
            print("Error while inserting the data to the table", error)
            continue

        print("Job submitted at {} \n".format(pd.Timestamp.today()))
        # id += 1


if __name__ == "__main__":
    main()
