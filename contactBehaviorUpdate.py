#!/usr/bin/env python

import csv, json
import os, sys
from timeit import default_timer as timer
from time import time
from confluent_kafka import Producer

#File Source Variables
projectPath="/Users/ian.salandy@ibm.com/PycharmProjects/kafkaProducer/"
payLoadFile="payload.json"
recipientsFile="recipAC_3985_1M.csv"
payLoadListFile="payloadList.json"

try:
    payloadCount = int(input("Enter the number of unique payloads to send:"))
except ValueError:
    print("Not a valid number, try again:")

#Kafka Cluster variables
broker = ["b-1.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092",\
    "b-2.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092",\
    "b-3.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092",\
    "b-4.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092",\
    "b-5.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092",\
    "b-6.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:9092"]

brokerCollection = ','.join(broker)

zookeeper = ["z-1.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:2181",\
    "z-2.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:2181",\
    "z-3.campaign-qa-ld2-msk.t8y33u.c8.kafka.us-east-1.amazonaws.com:2181"]

zookeeperCollection = ','.join(zookeeper)

topic = "wca.qald2.contactBehaviorUpdate"

#Read Payload Template
with open(projectPath + payLoadFile, "r") as jsonReadFile:
    readJSON = json.load(jsonReadFile)
    jsonReadFile.close

#Read Recipient data (listID2 and recipient ID)
with open(projectPath + recipientsFile) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')

    #Create payload list file, write populated payload template line by line
    with open(projectPath + payLoadListFile, "a") as jsonAppendFile:
        jsonAppendFile.truncate(0)

        loopCount = 0
        p = Producer({'bootstrap.servers': brokerCollection})

        start = timer()     #Start send timer
        for row in readCSV:

            readJSON["recipientId"] = row[1]
            readJSON["contactId"] = row[1]
            readJSON["listId"] = row[0]
            readJSON["updated"] = int(time() * 1000)
            readJSON["lastClicked"] = int(time() * 1000)
            readJSON["lastSent"] = int(time() * 1000)
            readJSON["lastOpened"] = int(time() * 1000)
            readJSON["eventDate"] = int(time() * 1000)

            json.dump(readJSON, jsonAppendFile)
            jsonAppendFile.write("\n")

            #time.sleep(.25)
            p.produce(topic, json.dumps(readJSON))
            p.flush()

            loopCount+=1
            if loopCount == int(payloadCount):
                #os.system("kafkacat -b {} -t {} -T -P -l {}".format(brokerCollection,topic,projectPath+payLoadListFile))
                end = timer()       #Stop send timer
                print(end - start)      #Stop send timer
                break
