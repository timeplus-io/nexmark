-- Average Selling Price by Seller
SELECT
  seller, array_avg(concat([final], lags(final, 1, 9, 0))) OVER (PARTITION BY seller)
FROM
  (
    SELECT
      max(B.price) AS final, A.seller AS seller, B.date_time
    FROM
      auction AS A
    INNER JOIN bid AS B ON A.id = B.auction
    WHERE
      (B.date_time >= A.date_time) AND (B.date_time <= A.expires)
    GROUP BY
      A.id, A.seller, B.date_time
  )
SETTINGS
  seek_to = 'earliest';
