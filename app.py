from enum import unique
from os import stat
from flask import Flask, json, jsonify, abort, request, make_response, url_for, Response, send_from_directory
import asterisk.manager as astman
import sys
import re
import pymysql
import datetime
import time

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
        dtfrom = request.args.get('dtfrom', default=f"{datetime.date.today()} 00:00:00", type=toDateTime)
        dtto = request.args.get('dtto', default=f"{datetime.date.today()} 23:59:59", type=toDateTime)
        queue_name = request.args.get('queue', default = '%',)
        get_rate = request.args.get('get_rate', default='no')
        get_lostafter = request.args.get('lostafter', default=0, type=int)

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

        # lost after 40 sec waiting
        if get_lostafter > 0:
            ok, res = db_select(
                query=(
                    " SELECT count(*) "
                    " FROM queue_log "
                    f" WHERE (time BETWEEN '{dtfrom}' AND '{dtto}') "
                    " AND EVENT = 'ABANDON'"
                    " AND AGENT = 'NONE' "
                    f" AND data3 >= {get_lostafter}; "
                )
            )
            if not ok:
                abort(404)
            data["stat"]["lostafter"] = res[0][0]

        # total lost
        data["stat"]["lost"] = data["stat"]["received"] - data["stat"]["answered"]
        
    except:
        abort(404)

    return jsonify(data)


@app.route(f'{ROOT_URI}/stat/rate', methods = ['GET'])
def get_stat_rate():
    try:
        dtfrom = request.args.get('dtfrom', default=f"{datetime.date.today()} 00:00:00", type=toDateTime)
        dtto = request.args.get('dtto', default=f"{datetime.date.today()} 23:59:59", type=toDateTime)
        queue_name = request.args.get('queue', default = '%',)

        
        ok, res = db_select(
            query=(
                " SELECT t1.time, t1.queuename, t1.agent, t1.data1 as rate, t2.data2 as callerid "
                " FROM queue_log as t1 "
                " JOIN queue_log as t2 "
                " ON t1.callid = t2.callid "
                " WHERE  t2.event = 'ENTERQUEUE' "
                f" AND t1.queuename like '{queue_name}' "
                " AND t1.event = 'RATE' "
                f" AND t1.time BETWEEN '{dtfrom}' AND '{dtto}' ;"
            )
        )

        if not ok:
            abort(404)

        data = []
        for row in res:
            row = list(row)
            row[2] = row[2].split('/')[1]
            data.append(row)

        
    except:
        print(sys.exc_info())
        abort(404)

    return jsonify(data)
    

@app.route(f'{ROOT_URI}/stat/general', methods = ['GET'])
def get_stat_general():
    data = {
        "stat": {},
        "delay": 0,
    }
    ts1 = time.time()
    try:
        dtfrom = request.args.get('dtfrom', default=f"{datetime.date.today()} 00:00:00", type=toDateTime)
        dtto = request.args.get('dtto', default=f"{datetime.date.today()} 23:59:59", type=toDateTime)
        
        # dtfrom="2021-08-18T00:00:00"
        # dtto="2021-08-18T23:00:00"

        ok, agent_list = db_select(
            query = (
                "SELECT DISTINCT agent FROM `asterisk`.`queue_log` "
                " WHERE (event IN ('CONNECT', 'RINGNOANSWER'))"
                f"AND (time between '{dtfrom}' and '{dtto}') "
            )
        )
        if not ok:
            abort(404)

        ok, day_list = db_select(
            query = (
                "SELECT DISTINCT DATE(time) AS `day` FROM `asterisk`.`queue_log` "
                " WHERE (event IN ('CONNECT', 'RINGNOANSWER'))"
                f"AND (time between '{dtfrom}' and '{dtto}') "
            )
        )
        if not ok:
            abort(404)

        # init dict with days and agents
        for t1_row in day_list:
            day = t1_row[0].strftime("%d-%m-%Y")
            data["stat"][day] = {}
            for t2_row in agent_list:
                agent = t2_row[0].split("/")[1]
                data["stat"][day][agent] = {
                    "sent": 0,
                    "accepted": 0,
                    "missed": 0,
                    "avg_rate": 0,
                    "talk_time": 0,
                    "avg_hold": 0,
                    "wrapup_time": 0,
                    "wrapup_per_call": 0,
                    "work_time": 0,
                }

        # accepted
        ok, res = db_select(
            query=(
                "SELECT DATE(time) as `day`, agent, count(1) as accepted, avg(data1) as avg_hold FROM `asterisk`.`queue_log` "
                "WHERE (event = 'CONNECT') "
                f"AND (time between '{dtfrom}' and '{dtto}') "
                "GROUP BY `day`, agent "
                "ORDER BY agent, `day`; "
            )
        )
        if not ok:
            abort(404)

        for row in res:
            day = row[0].strftime("%d-%m-%Y")
            agent = row[1].split('/')[1]  # split SIP/0001 to SIP and 0001
            accept_cnt = row[2]
            avg_hold = round(row[3])
            data["stat"][day][agent]["accepted"] = accept_cnt
            data["stat"][day][agent]["avg_hold"] = avg_hold

        # missed
        ok, res = db_select(
            query=(
                "SELECT DATE(time) as `day`, agent, count(1) as missed FROM `asterisk`.`queue_log`  "
                "WHERE (event = 'RINGNOANSWER') "
                f"AND (time between '{dtfrom}' and '{dtto}') "
                "GROUP BY `day`, agent "
                "ORDER BY agent, `day` "
            )
        )
        if not ok:
            abort(404)

        for row in res:
            day = row[0].strftime("%d-%m-%Y")
            agent = row[1].split('/')[1]  # split SIP/0001 to SIP and 0001
            missed_cnt = row[2]
            data["stat"][day][agent]["missed"] = missed_cnt
            data["stat"][day][agent]["sent"] = missed_cnt + data["stat"][day][agent]["accepted"]

        # avg rate
        ok, res = db_select(
            query=(
                "SELECT DATE(time) as `day`, agent, AVG(data1) as avgrate FROM `asterisk`.`queue_log`  "
                "WHERE (event = 'RATE') "
                f"AND (time between '{dtfrom}' and '{dtto}') "
                "GROUP BY `day`, agent "
                "ORDER BY agent, `day` ;"
            )
        )
        if not ok:
            abort(404)

        for row in res:
            day = row[0].strftime("%d-%m-%Y")
            agent = row[1].split('/')[1]  # split SIP/0001 to SIP and 0001
            avg_rate = row[2]
            data["stat"][day][agent]["avg_rate"] = round(avg_rate,1)


        # talk time
        ok, res = db_select(
            query=(
                "SELECT DATE(time) as `day`, agent, SUM(data2) as talktime FROM `asterisk`.`queue_log`  "
                "WHERE (event in ('COMPLETEAGENT', 'COMPLETECALLER')) "
                f"AND (time between '{dtfrom}' and '{dtto}') "
                "GROUP BY `day`, agent "
                "ORDER BY agent, `day` "
            )
        )
        if not ok:
            abort(404)

        for row in res:
            day = row[0].strftime("%d-%m-%Y")
            agent = row[1].split('/')[1]  # split SIP/0001 to SIP and 0001
            talk_time = row[2]
            data["stat"][day][agent]["talk_time"] = talk_time


        # wrapup time (pause between calls)
        for agent_row in agent_list:
            agent = agent_row[0].split('/')[1]

            ok, res = db_select(
                query = (
                    "SELECT time, DAY(time), agent, event FROM `asterisk`.`queue_log`  "
                    "WHERE (event in ('CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER')) "
                    f"AND (agent like '%/{agent}') "
                    f"AND (time between '{dtfrom}' and '{dtto}') "
                    "ORDER BY time "
                )
            )

            if not ok:
                abort(404)

            prev = res[0]
            first = res[0]

            for row in res:
                if (first[1] != row[1]) or (res.index(row)+1 == len(res)):  # if current is the next day or last record in list
                    # print(f"agent: {agent}, first: {first[0]}, last: {prev[0]}")
                    delta = prev[0] - first[0]
                    day = prev[0].strftime("%d-%m-%Y")   
                    data["stat"][day][agent]["work_time"] = delta.seconds
                    data["stat"][day][agent]["wrapup_time"] = data["stat"][day][agent]["work_time"] - data["stat"][day][agent]["talk_time"]
                    data["stat"][day][agent]["wrapup_per_call"] = round(data["stat"][day][agent]["wrapup_time"] / data["stat"][day][agent]["accepted"])
                    # prepeare for the next day
                    first = row
                prev = row
                
    except:
        print(sys.exc_info())
        abort(404)
    data["delay"] = time.time() - ts1
    
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