-- Monitor New Users
SELECT
  P.id, P.name, P.window_start
FROM
  (
    SELECT
      id, name, window_start, window_end
    FROM
      tumble(person, date_time, 10s)
    GROUP BY
      id, name, window_start, window_end
  ) AS P
INNER JOIN (
    SELECT
      seller, window_start, window_end
    FROM
      tumble(auction, date_time, 10s)
    GROUP BY
      seller, window_start, window_end
  ) AS A ON P.id = A.seller
SETTINGS
  seek_to = 'earliest'