import sqlite3
import subprocess
import pandas as pd
from datetime import datetime, date, timedelta
import time
import math

runTimes = []
global jobs
global blocks

# Example variable for testing
# Energy distribution for each stage represented as [(duration, average power)], duration is in seconds
# Power draw equals average CPU energy usage

example_energy = [(257,206),(135,88),(125,77),(943,90),(410,71),(485,123),(153,69),(574,79),(93,44)]

def main():
    updateQueue()
    while not (jobs.empty):
        updateQueue()
        updatePredictions()
        updateTargets()
        executeDeadlines()
        print("Current job optimized execution times (id, execution time): \n")
        for run in runTimes:
            print(run)
        print("\n")
        print("Waiting... " + str(pd.Timestamp.now()) + "\n")
        time.sleep(60)
    print("No jobs present in queue " + str(pd.Timestamp.now()) + " waiting...\n")
    time.sleep(60)
    main()


def executeDeadlines():
    for index, row in jobs.iterrows():
        for idx, run in enumerate(runTimes):
            if run[0] == row["id"]:
                if pd.Timestamp.today() > run[1]:
                    # subproccess which detaches from parent process as a child process to submit job to cluster
                    spark_submit_str = "gcloud dataproc jobs submit pyspark --cluster=cluster-4ba8 --region europe-west2 gs://gti-bucket1-dataproc/notebooks/jobs/{0} -- {1}".format(
                        row["filename"], row["args"]
                    )
                    process = subprocess.Popen(
                        spark_submit_str,
                        stdin=None,
                        stdout=None,
                        stderr=None,
                        close_fds=True,
                        universal_newlines=True,
                        shell=True,
                    )

                    jobs.drop(index)
                    runTimes.pop(idx)
                    deleteJob(row)
                    # Recursively call function such that indexes for remaining jobs are not misinterpreted
                    executeDeadlines()


def deleteJob(id):
    connection = sqlite3.connect("test.db")
    cursor = connection.cursor()
    cursor.execute("DELETE FROM jobs WHERE id = ?;", [id["id"]])
    connection.commit()
    updateQueue()
    print(
        "Job with id '{}' removed from database as its been submitted to the cluster\n".format(
            id["id"]
        )
    )
    print(
        "Time of submission: {} Time of execution: {}".format(
            pd.to_datetime(id["submissionTime"]), pd.Timestamp.today()
        )
    )
    logs = open("logs.txt", "a")
    logs.write("\n")
    logs.write(str(id) + " was executed at " + str(pd.Timestamp.today()) + "\n")
    logs.flush()


def updateQueue():
    global jobs
    # import update from database
    connection = sqlite3.connect("test.db")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM jobs")
    rows = cursor.fetchall()
    jobs = pd.read_sql("SELECT * FROM jobs", connection)
    if jobs.empty:
        return
    print("Current jobs in queue:\n")
    print(jobs)
    print("\n")


def updatePredictions():
    # predictions are currently configured for using  national carbon intensity API
    # this can be changed to the country specific carbon intensity API by changing the url below, commenting on line 106
    # then finally commenting line 121 and uncommenting line 122

    global blocks
    # url = "https://data.nationalgrideso.com/backend/dataset/d8084aa3-8c9e-425c-bf0d-51e0441fc241/resource/e032e7aa-dea5-4695-80d6-19739142021d/download/country_carbon_intensity.csv"
    url = "https://data.nationalgrideso.com/backend/dataset/f406810a-1a36-48d2-b542-1dfb1348096e/resource/0e5fde43-2de7-4fb4-833d-c7bca3b658b0/download/gb_carbon_intensity.csv"
    df = pd.read_csv(url, on_bad_lines="skip")
    day = pd.Timestamp.today() + timedelta(days=1)
    df["datetime"] = pd.to_datetime(df["datetime"])
    # Select the forecast for the next 24 hours

    next_day = df[
        (df["actual"].isnull())
        & (
            (
                df["datetime"]
                > (
                    roundDown(
                        datetime.now(), timedelta(minutes=30) - timedelta(minutes=1)
                    )
                )
            )
            & (df["datetime"] < pd.Timestamp(day))
        )
    ]

    # chunks = next_day["England"].tolist()
    chunks = next_day["forecast"].tolist()

    blocks = []
    now = datetime.now()
    # Convert from 30 minute blocks into minute blocks
    for i in range(len(chunks)):
        for j in range(30):
            blocks.append(chunks[i])
    # remove blocks that are in the past

    blocks = blocks[(now.minute % 30) :]


# functions for rounding up or down to nearest 30 minute mark
def roundUp(time, delta):  # make round down for next blocks
    rounded = datetime.min + math.ceil((time - datetime.min) / delta) * delta
    return pd.Timestamp(rounded)


def roundDown(time, delta):
    rounded = datetime.min + math.floor((time - datetime.min) / delta) * delta
    return pd.Timestamp(rounded)


def lowestCarbon(job): #lowest carbon emission positioning considering energy profile
    earliest = roundUp(datetime.now(), timedelta(minutes=30))
    latest = pd.to_datetime(job["deadline"]) - timedelta(minutes=float(job["runtime"]))
    # 25% buffer to account for potential extra runtime due to DVFS actions
    buffer = timedelta(minutes=float(job["runtime"])) // 4

    # window job can be executed i.e. window for shifting
    window = latest - earliest - buffer
    minutes = int(window.total_seconds() / 60)

    # number of minutes i.e. no of blocks in this window
    blockLimit = round(minutes, 1)

    jobSize = int(job["runtime"])

    mins = 0
    best = 1000000000

    stageOrder = stageTimeAcculator()

    for i in range(blockLimit):
        total = 0
        stagePosition = 0

        # check there is still room left for checks
        if i > blockLimit - jobSize:
            break

        for j in range(jobSize):
            if (j > stageOrder[stagePosition]):
                # split minute into two piece for different stages then apply energy draw to each respective minute
                total = total + blocks[i+j] * example_energy[stagePosition][1] * math.modf(stageOrder[stagePosition])[0]
                total = total + blocks[i+j+1] * example_energy[stagePosition+1][1] * (1- math.modf(stageOrder[stagePosition])[0])
            else:                
                total = total + blocks[i+j] * example_energy[stagePosition][1]
            if best > total:
                best = total
                mins = i
    return earliest + timedelta(minutes=mins)


    # for i in range(blockLimit):
    #     total = 0

    #     # check there is still room left for checks
    #     if i > blockLimit - jobSize:
    #         break
    #     for j in range(jobSize):
    #         total = total + blocks[i + j]

    #     if best > total:
    #         best = total
    #         mins = i
    # return earliest + timedelta(minutes=mins)


def updateTargets():
    if len(runTimes) < len(jobs):
        for index, row in jobs.iterrows():
            present = False
            for run in runTimes:
                if run[0] == row["id"]:
                    present = True
            if not present:
                runTimes.append(
                    (row["id"], (pd.Timestamp(row["deadline"]))),
                )

    for index, row in jobs.iterrows():
        for run in runTimes:
            if run[0] == row["id"]:
                executionTime = run[1]
        if not executionTime:
            executionTime = pd.Timestamp(row["deadline"])
        # if the job is scheduled to be executed in the next 5 minutes break
        if pd.Timestamp.now() > executionTime - timedelta(minutes=5):
            break
        startPos = lowestCarbon(row)

        present = False
        # runTimes is a tuple with id and target runtime time
        for index, run in enumerate(runTimes):
            if run[0] == int(row["id"]):
                runTimes[index] = (row["id"], startPos)

def stageTimeAcculator():
    total = 0
    stageOrder = []
    for i in range(len(example_energy)):
        total += example_energy[i][1] / 60
        stageOrder.append(total)

    return stageOrder

if __name__ == "__main__":
    main()
