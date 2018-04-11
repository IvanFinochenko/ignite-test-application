SELECT DISTINCT s.subsKey, cw.cuncInd, cwFriend.name
FROM SUBSCRIBER s JOIN CALL c ON s.subsKey = c.subsFrom
JOIN CARWASH cw ON cw.subsKey = c.subsTo
LEFT JOIN (
      SELECT place, name
      FROM CARWASH
      WHERE cuncInd = 1
      LIMIT 1) cwFriend
   ON cwFriend.place = cw.place
WHERE c.dur >= 60 AND s.timeKey < ?