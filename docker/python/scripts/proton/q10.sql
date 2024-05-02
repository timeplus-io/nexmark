SELECT
  _tp_time, auction, bidder, price, date_time, extra, format_datetime(date_time, '%Y-%m-%d'), format_datetime(date_time, '%H:%m')
FROM
  bid
WHERE
  _tp_time > earliest_ts()