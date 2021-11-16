from enum import unique
from os import stat
from flask import Flask, json, jsonify, abort, request, make_response, url_for, Response, send_from_directory
import asterisk.manager as astman
import sys
import re
import pymysql
import datetime

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
                'calls': int(re.findall(r'Calls:\W(\d+)\r\n', event)[0]),  # calls in queue
                'calls_connected': 0,                                      # current connected calls
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
                'lastcall': datetime.datetime.utcfromtimestamp(int(re.findall(r'LastCall:\W(\d+)\r\n', event)[0])),
            }
            # count calls connected 
            if summary[q_name]['agents'][member_name]['incall'] == 'yes':
                summary[q_name]['calls_connected'] += 1
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
    # TODO
    data = {
        'status': 'ok',
        'version': '0.0.1',
        'uptime': '185d',
    }
    return jsonify(data)


@app.route(f'{ROOT_URI}/queues/brief', methods = ['GET'])
def get_queues():
    try:
        data = {
            "queues": man_get_queues_summary(),
            "debug_info": 2146,
        }
    except:
        abort(404)

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
def get_mp3(uniqueid):
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
    mp3directory = f"{RECORDS_ROOT}/{dt.year}/{month}/{day}"
    mp3name = f"{row[0]}"
    return send_from_directory(mp3directory, mp3name)


def toDateTime(dateString): 
    # return datetime.datetime.strptime(dateString, "%Y-%m-%d").date()
    # TODO
    return dateString


@app.route(f'{ROOT_URI}/agent/history', methods = ['GET'])
def get_agent_history():
    data = {
        "result": False,
        "history": {},
    }
    try:
        dtfrom = request.args.get('dtfrom', default = datetime.date.today(), type = toDateTime)
        dtto = request.args.get('dtto', default = datetime.date.today(), type = toDateTime)
        agent_name = request.args.get('agent')
        
        # agent statuses history
        # debug
        # dtfrom = "2021-09-15T00:00:00"
        # dtto = "2021-09-15T23:00:00"
        # agent_name = "0006"

        ok, res = db_select(
            query=(
                "SELECT * "
                "FROM agent_status_history "
                f"WHERE (timestamp BETWEEN '{dtfrom}' AND '{dtto}') "
                f"AND agentId = 'SIP/{agent_name}' "
            )
        )


        if ok:
            data["result"] = True
            data["row_count"] = len(res)
            for row in res: 
                data["history"].update(
                    {row[0]: {
                            "timestamp": row[3],
                            "status": row[2],
                            "queue": row[4], 
                        }
                    }
                )

    except:
        abort(404)

    return jsonify(data)


@app.route(f'{ROOT_URI}/stat/totals', methods = ['GET'])
def get_stat_totals():
    data = {
        "stat": {},
    }
    try:
        dtfrom = request.args.get('dtfrom', default = f"{datetime.date.today()} 00:00:00", type = toDateTime)
        dtto = request.args.get('dtto', default = f"{datetime.date.today()} 23:59:59", type = toDateTime)
        queue_name = request.args.get('queue', default = '%',)
        get_rate = request.args.get('get_rate', default='no')

        # total received
        # dtfrom="2021-08-18T00:00:00"
        # dtto="2021-08-18T23:00:00"
        ok, res = db_select(
            query=(
                " SELECT count(*) "
                " FROM queue_log "
                f" WHERE (time BETWEEN '{dtfrom}' AND '{dtto}') "
                f" AND (queuename like '{queue_name}') "
                " AND EVENT = 'ENTERQUEUE' "
            )
        )
        if not ok:
            abort(404)
        data["stat"]["received"] = res[0][0]
        
        # total answered & avg(wait)
        ok, res = db_select(
            query=(
                " SELECT count(*), avg(data1) "
                " FROM queue_log "
                f" WHERE (time BETWEEN '{dtfrom}' AND '{dtto}') "
                f" AND (queuename like '{queue_name}') "
                " AND event = 'CONNECT' "
            )
        )
        if not ok:
            abort(404)
        data["stat"]["answered"] = res[0][0]
        data["stat"]["avg_wait"] = res[0][1]

        # rating
        if get_rate == 'yes':
            ok, res = db_select(
                query=(
                    " SELECT count(*), avg(data1) "
                    " FROM queue_log "
                    f" WHERE (time BETWEEN '{dtfrom}' AND '{dtto}') "
                    f" AND (queuename like '{queue_name}') "
                    " AND event = 'RATE' "
                    " AND data1 != '0' "
                )
            )
            if not ok:
                abort(404)
            data["stat"]["rate_count"] = res[0][0]
            data["stat"]["avg_rate"] = res[0][1]


        # total lost 
        data["stat"]["lost"] = data["stat"]["received"] - data["stat"]["answered"]
        
    except:
        abort(404)

    return jsonify(data)
    

@app.route(f'{ROOT_URI}/agent/pause/<string:agent_name>', methods = ['GET', 'POST'])
def agent_pause(agent_name):
    data = {
        'result': False,
        'agent': agent_name,
    }
    if request.method == 'GET':
        summary = man_get_queues_summary()
        agent_queues = []
        data['paused_queues'] = []
        for q_name, q_info in summary.items():
            if agent_name in q_info['agents'].keys():
                agent_queues.append(q_name)
                if q_info['agents'][agent_name]['paused'] == 'yes':
                    data['paused_queues'].append(q_name)

        if agent_queues:
            data['result'] = True
            data['paused'] = {True: 'yes', False: 'no'}.get(len(data['paused_queues']) > 0)
            
        return jsonify(data)
    
    # POST
    try:
        if not request.form['paused'] in ['yes', 'no']:
            raise
        action = {
            'Action':'QueuePause',
            'Interface': f'SIP/{agent_name}',
            'Paused': {'yes': 'true', 'no': 'false'}.get(request.form['paused']),
        }
        if 'queue' in request.form.keys():
            action['Queue': request.form['queue']]

        res = manager.send_action(action)
        data['result'] = res.headers['Response'] == 'Success'
        data['msg'] = res.headers['Message']
    except:
        print(sys.exc_info())

    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6412, debug = True)