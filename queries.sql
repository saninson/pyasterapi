-- get list of active agents
SELECT DISTINCT agent FROM `asterisk`.`queue_log` 
WHERE (event IN ('CONNECT', 'RINGNOANSWER'))
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')

-- get list of days having events
SELECT DISTINCT DAY(time) AS `day` FROM `asterisk`.`queue_log` 
WHERE (event IN ('CONNECT', 'RINGNOANSWER'))
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')

-- accepted by an agent
SELECT DAY(time) as `day`, agent, count(1) as accepted, avg(data1) as avg_hold  FROM `asterisk`.`queue_log` 
WHERE (event = 'CONNECT')
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')
GROUP BY `day`, agent
ORDER BY agent, `day`
LIMIT 100;

-- missed by an agent
SELECT DAY(time) as `day`, agent, count(1) as missed FROM `asterisk`.`queue_log` 
WHERE (event = 'RINGNOANSWER')
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')
GROUP BY `day`, agent
ORDER BY agent, `day`
LIMIT 100;

-- avg rate by agent
SELECT DAY(time) as `day`, agent, AVG(data1) as avgrate FROM `asterisk`.`queue_log` 
WHERE (event = 'RATE')
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')
GROUP BY `day`, agent
ORDER BY agent, `day`
LIMIT 100;

-- talk time
SELECT DAY(time) as `day`, agent, SUM(data2) as talktime FROM `asterisk`.`queue_log` 
WHERE (event in ('COMPLETEAGENT', 'COMPLETECALLER'))
AND (time between '2021-10-01 00:00:00' and '2021-10-31 23:59:59')
GROUP BY `day`, agent
ORDER BY agent, `day`
LIMIT 100;

-- post call processing
SELECT time, DAY(time) as `d`, TIME(time) as `t`, agent, event FROM `asterisk`.`queue_log` 
WHERE (event in ('CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER'))
AND (agent = 'SIP/0001')
AND (time between '2021-10-01 00:00:00' and '2021-10-05 23:59:59')
ORDER BY time
