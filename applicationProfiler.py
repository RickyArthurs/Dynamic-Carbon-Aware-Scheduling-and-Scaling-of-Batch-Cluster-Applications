import pandas as pd
from pandas import json_normalize
import sys
import sqlite3
from datetime import datetime, date, timedelta


def main():
    # Check if the correct number of arguments are provided
    if len(sys.argv) != 5:
        print("Usage: python applicationProfiler.py sparklogfile powerlogfile jobID nodes")
        return

    # Extract arguments
    spark_logs = sys.argv[1]
    power_logs = sys.argv[2]
    
    # Convert integer arguments to integers
    try:
        jobID = int(sys.argv[3])
        nodes = int(sys.argv[4])
    except ValueError:
        print("Error: Integers expected for int1 and int2")
        return
    
    utilisation_profile, power_profile = processPowerLogs(power_logs)
    dvfs_plan, stages, energy_consumption_profile = processSparkLogs(spark_logs, utilisation_profile)
    sendToDatabase(dvfs_plan, energy_consumption_profile, nodes, jobID)




def processPowerLogs(power_logs):
    power_logs_data = pd.read_csv(power_logs)

    # maximum utilisation value
    threshold = 100
    # Extract and filter CPU utilisation
    power_logs_data = power_logs_data[power_logs_data[' CPU Utilization(%)'] <= threshold]
    power_logs_data = power_logs_data.reset_index(drop=True)
    utilisation_profile = power_logs_data
    # Extract data for power usage
    power_profile = power_logs_data[['Elapsed Time (sec)', 'Processor Power_0(Watt)']]

    return utilisation_profile, power_profile

def processSparkLogs(spark_logs, utilisation_profile):
    # process spark log

    spark_logs = pd.read_json(spark_logs, lines=True)

    stages = spark_logs.loc[(spark_logs['Event'] == "SparkListenerStageCompleted")]

    stages = json_normalize(stages['Stage Info'])

    stages['Submission Time'] = pd.to_datetime(stages['Submission Time'], unit='ms')
    stages['Completion Time'] = pd.to_datetime(stages['Completion Time'], unit='ms')

    columns_req = ['Submission Time','Completion Time']

    sorted_stages = stages[columns_req]

    # Get power logs timeframe
    power_times = utilisation_profile['System Time']

    # Get spark logs stage timeframes
    sorted_stages['Submission Time'] = sorted_stages['Submission Time'].dt.time
    sorted_stages['Completion Time'] = sorted_stages['Completion Time'].dt.time

    # Assign undetermined stage for each time
    utilisation_profile['StageID'] = -1

    # Determine each time periods stage
    for i in range(len(power_times)):
        for j in range(len(sorted_stages)):
            if (power_times[i] > sorted_stages.loc[j, 'Submission Time']) and (power_times[i] < sorted_stages.loc[j, 'Completion Time']):
                utilisation_profile.loc[i, 'StageID'] = j
            else:
                continue
    # Disregard time stamps in power log outside of job window
    utilisation_profile = utilisation_profile[utilisation_profile['StageID'] != -1]

    stages = utilisation_profile['StageID'].tolist()

    # Calculate energy consumption profile
    energy_distribution = energyConsumptionProfile(utilisation_profile, power_times)

    return utilisation_profile, stages, energy_distribution

def energyConsumptionProfile(utilisation_profile):
    energy_consumption_profile = []
    stage = 0
    total = 0
    counter = 0
    for i in range(len(utilisation_profile)):
        if (utilisation_profile.loc[i, 'StageID'] == stage):
            total += utilisation_profile.loc[i, 'StageID']
            counter += 1
        else:
            stage += 1
            average = total / counter
            # convert counter from 100ms to 1 second intervals, as this is granularity of power logs
            counter = counter / 10
            energy_consumption_profile.append((counter, average))
            counter = 0
    return energy_consumption_profile

        
def sendToDatabase(dvfs_plan, energy_consumption_profile, nodes, jobID):
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    # remove old optimised plan if present
    cursor.execute("DROP TABLE IF EXISTS `{}`;".format(jobID))
    # create table statement for each job
    create_statement = """CREATE TABLE jobs (
    dvfs_plan DOUBLE,
    energy DOUBLE,
    nodes int
    );"""
    cursor.execute(create_statement)

    #then add values where each row represents a stage, expect for nodes column



if __name__ == "__main__":
    main()