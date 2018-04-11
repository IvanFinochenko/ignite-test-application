SELECT subs_from, subs_to, dur, start_time
FROM Call
WHERE start_time > ?
AND start_time < ?