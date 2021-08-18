from enum import unique
from flask import Flask, jsonify, abort, request, make_response, url_for, Response
import asterisk.manager as astman
import sys
import re
import pymysql
from datetime import datetime

app = Flask(__name__, static_url_path = "")

MANAGER_HOST = 'localhost'
MANAGER_PORT = 5038
MANAGER_USER = 'api'
MANAGER_SECRET = 'jHKTeIoY3fFZA9D'

ROOT_URI = '/pbx/api'
RECORDS_ROOT = '/var/calls'

DB_HOST = 'localhost'
DB_USER = 'asterisk'
DB_PASS = 'SZ2tqeTw7AO2'
DB_DBNAME = 'asterisk'

manager = astman.Manager()
manager.connect(MANAGER_HOST)
manager.login(MANAGER_USER, MANAGER_SECRET)


def man_get_queues_summary():
    res = manager.send_action({'Action': 'QueueStatus',})
    events = res.data.strip().split('\r\n\r\n')
    summary = {}

    for event in events:
        if 'QueueParams' in event:
            q_name = re.findall(r'Queue:\W(.+)\r\n', event)[0]
            summary[q_name] = {
                'holdtime': int(re.findall(r'Holdtime:\W(\d+)\r\n', event)[0]),
                'talktime': int(re.findall(r'TalkTime:\W(\d+)\r\n', event)[0]),
                'completed': int(re.findall(r'Completed:\W(\d+)\r\n', event)[0]),
                'calls': int(re.findall(r'Calls:\W(\d+)\r\n', event)[0]),
                'paused': 0,
                'online': 0,
                'ready': 0,
                'agents': {},
            }
        elif 'QueueMember' in event:
            q_name = re.findall(r'Queue:\W(.+)\r\n', event)[0]
            member_name = re.findall(r'\r\nName: SIP/(\w+)\r\n', event)[0]
            summary[q_name]['agents'][member_name] = {
                'paused': {'1': 'yes', '0': 'no'}.get(re.findall(r'Paused:\W(\d+)\r\n', event)[0]),
                'status': int(re.findall(r'Status:\W(\d+)\r\n', event)[0]),
                'status_desc': {
                    '0': 'unknown',
                    '1': 'not_in_use',
                    '2': 'in_use',
                    '3': 'busy',
                    '4': 'invalid',
                    '5': 'unavailable',
                    '6': 'ringing',
                    '7': 'ring_in_use',
                    '8': 'on_hold'}.get(re.findall(r'Status:\W(\d+)\r\n', event)[0]),
                'incall': {'1': 'yes', '0': 'no'}.get(re.findall(r'InCall:\W(\d+)\r\n', event)[0]),
                'lastcall': datetime.utcfromtimestamp(int(re.findall(r'LastCall:\W(\d+)\r\n', event)[0])),
            }
            # count paused
            if summary[q_name]['agents'][member_name]['paused'] == 'yes':
                summary[q_name]['paused'] += 1
            # count online
            if summary[q_name]['agents'][member_name]['status'] in [1, 2, 3, 6, 7, 8]:
                summary[q_name]['online'] += 1
                # count ready
                if summary[q_name]['agents'][member_name]['status'] == 1:
                    summary[q_name]['ready'] += 1 

    return summary
    

@app.route(f'{ROOT_URI}/status', methods = ['GET'])
def get_status():
    data = {
        'status': 'ok',
        'version': '0.0.1',
        'uptime': '185d',
    }
    return jsonify(data)


@app.route(f'{ROOT_URI}/queues/brief', methods = ['GET'])
def get_queues():
    man_get_queues_summary()
    try:
        data = {
            'status': 'ok',
            'queues': man_get_queues_summary(),
        }
    except:
        data = {
            'status': 'false',
        }
    return jsonify(data)


def db_select(query):
    try:
        con = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_DBNAME)
        cur = con.cursor()
        cnt = cur.execute(query)
        ok = (cnt != 0)
        return ok, cur.fetchall()
    except:
        return False, None


@app.route("/mp3/<float:uniqueid>")
def stream_mp3(uniqueid):
    # find file name in db
    ok, res =  db_select(
        query = (
            "SELECT recordingfile, calldate "
            "FROM cdr "
            f"WHERE uniqueid = {uniqueid} AND recordingfile != ''"
        )
    )
    if not ok:
        abort(404)

    row = res[0] 
    dt = row[1]
    month = f"0{dt.month}"[-2:]
    day = f"0{dt.day}"[-2:]
    # buld file path using record creation date
    mp3path = f"{RECORDS_ROOT}/{dt.year}/{month}/{day}/{row[0]}"

    def generate():
        with open(mp3path, "rb") as fmp3:
            data = fmp3.read(1024)
            while data:
                yield data
                data = fmp3.read(1024)
    return Response(generate(), mimetype="audio/mpeg")

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug = True)