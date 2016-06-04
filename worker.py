#!/usr/bin/python

'''
Exemplo recolhido na Internet e adaptado
'''

import boto3
from botocore.client import Config

import hashlib
from binascii import hexlify,unhexlify
import random
import boto3
import json
import urllib
import time
"""
header_hex = ("01000000" +
 "81cd02ab7e569e8bcd9317e2fe99f2de44d49ab2b8851ba4a308000000000000" +
 "e320b6c2fffc8d750423db8b1eb942ae710e951ed797f7affc8892b0f1fc122b" +

 "c7f5d74d" +
 "f2b9441a" +
 "42a14695")
header_bin = header_hex.decode('hex')
hash = hashlib.sha256(hashlib.sha256(header_bin).digest()).digest()
hash.encode('hex_codec')
'1dbd981fe6985776b644b173a4d0385ddc1aa2a829688d1e0000000000000000'
hash[::-1].encode('hex_codec')
'00000000000000001e8d6829a8a21adc5d38d0a473b144b6765798e61f98bd1d'
"""
sqs = boto3.resource('sqs')  # SQS

# Get the queue
queue = sqs.get_queue_by_name(QueueName='queue')


s3 = boto3.resource('s3')  # S3


sdb = boto3.client('sdb')  # Simple DB


def decode_boto3(body,file_name):

    filename = file_name + ".txt"


    s3.Bucket('eu-west-1-mgreis-es-instance1').put_object(Key=filename, Body=body)

    return "https://s3-eu-west-1.amazonaws.com/eu-west-1-mgreis-es-instance1/" + filename

def delete_boto3(url):
    s3.Object('eu-west-1-mgreis-es-instance1', url).delete()

def delete_job(job_id):
    while True:
        answer = sdb.select(SelectExpression="select * from ES2016")
        string = []

        if "Items" in answer:
            string = answer.get("Items")[0].get("Attributes")
            string2 = ""
            for i in string:

                if str(i.get('Name')) == job_id:
                    string2 = str(i.get('Value'))

            sdb.delete_attributes(DomainName="ES2016", ItemName='jobs',
                                  Attributes=[{'Name': job_id, 'Value': string2}])
            return






def find_proof_of_work(chain,number_of_zeros):
    target = gen_target(number_of_zeros)
    random_number = ''.join(random.choice('0123456789abcdef') for n in range(64))
    while hash_it(chain, random_number)>target:
        random_number = ''.join(random.choice('0123456789abcdef') for n in range(64))
    return random_number

def gen_target(number_of_zeros):
    target = ''.join('0' for n in range(number_of_zeros))
    target = target + ''.join('f' for n in range(64-number_of_zeros))
    return target

def get_file_contents(filename):

    response = urllib.request.urlopen(filename)
    data = response.read()  # a `bytes` object
    text = data.decode('utf-8')  #
    return text

def get_message_body():
    while (True):
        message_array = queue.receive_messages( WaitTimeSeconds=10)
        if message_array is not None:
            for message in message_array:
                print("Fetching:")
                if message is not None:
                    out = message.body
                    out = out.replace("'", "\"")
                    message.delete()
                    return json.loads(out)

def get_sdb(job_id):
    while (True):
        answer = sdb.select(SelectExpression="select * from ES2016")
        string = []

        if "Items" in answer:
            string = answer.get("Items")[0].get("Attributes")
        else:
            string = []

        for i in string:
            if i.get('Name')==job_id:
                out = json.dumps(i.get('Value'))
                out = out.replace("\"", "")
                out = out.replace("'", "\"")
                return json.loads(out)

def hash_it(chain, value):
    input_string = chain + value
    input_string_bin = unhexlify(input_string)
    result = hashlib.sha256(hashlib.sha256(input_string_bin).digest()).digest()
    return str(hexlify(result).decode('ascii'))



def post_sdb_finished(job_submitted, file_name,start_time):
    curr_time = str(int(round(time.time() * 1000)))

    value = str({"job_id": str(job_submitted), "job_state": "finished", "job_submitted": str(job_submitted), "job_started": start_time,"job_finished": curr_time, "job_file": str(file_name)})
    sdb.put_attributes(DomainName="ES2016", ItemName='jobs',
                     Attributes= [{'Name': str(job_submitted), 'Value': value}])  # new job in SDB
    return value

def post_sdb_started(job_submitted, file_name):
    curr_time = str(int(round(time.time() * 1000)))

    value = str({"job_id": str(job_submitted), "job_state": "started", "job_submitted": str(job_submitted), "job_started": curr_time,"job_finished": "-1", "job_file": str(file_name)})
    sdb.put_attributes(DomainName="ES2016", ItemName='jobs',
                     Attributes= [{'Name': str(job_submitted), 'Value': value}])  # new job in SDB
    return curr_time






def do_some_work(message_body):
    if  message_body is not None:
        print (message_body)
        message_body = json.loads(message_body.replace("'", "\""))

        print("Job file:      "+message_body.get('job_file'))
        print("Job id:        "+message_body.get('job_id'))
        database = get_sdb(message_body.get('job_id'))
        if database is not None:
            #print (database.get('job_id'))
            random_string = get_file_contents(database.get('job_file'))
            print ("Random String: " +random_string)
            if random_string is not None:
                delete_job(message_body.get('job_id'))
                start_time = post_sdb_started(message_body.get('job_id'), message_body.get('job_file'))
                answer = find_proof_of_work(random_string, 5)
                delete_boto3(database.get('job_file'))
                print ("")
                print ("Answer")
                print('random: ' + random_string)
                print('answer: ' + answer)
                print('hash:   ' + hash_it(random_string, answer))
                body = random_string +'\n'+answer
                file_path = decode_boto3(body, database.get('job_id'))
                delete_job(message_body.get('job_id'))
                value = post_sdb_finished(message_body.get('job_id'), message_body.get('job_file'),start_time)
                return value


botoConfig = Config(connect_timeout=50, read_timeout=70)
swf = boto3.client('swf', config=botoConfig)

DOMAIN = "MyFinalSWF"
WORKFLOW = "MyFinalSWF2"
TASKNAME = "proof_of_work"
VERSION = "1"
TASKLIST = "MyFinalTaskList2"
WORKER = str(int(round(time.time() * 1000)))


while True:
        print("Listening for Decision Tasks")
        task = swf.poll_for_activity_task(
                                          domain=DOMAIN,
                                          taskList={'name': TASKLIST},
                                          identity= WORKER)
        if 'taskToken' not in task:
            print("Poll timed out, no new task.  Repoll")
        else:
            print("New task", task['taskToken'], "arrived with input = ", task['input'])
            response = do_some_work(task['input'])
            swf.respond_activity_task_completed(
                                                taskToken=task['taskToken'],
                                                result=str(response)
                                                )

            print("Task Done")


