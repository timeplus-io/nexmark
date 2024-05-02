SELECT
  to_date(date_time) AS day, 
  count(*) AS total_bids, 
  count_if(*, price < 10000) AS rank1_bids, 
  count_if(*, (price >= 10000) AND (price < 1000000)) AS rank2_bids, 
  count_if(*, price >= 1000000) AS rank3_bids, 
  count_distinct(bidder) AS total_bidders, 
  count_distinct_if(bidder, price < 10000) AS rank1_bidders, 
  count_distinct_if(bidder, (price >= 10000) AND (price < 1000000)) AS rank2_bidders, 
  count_distinct_if(bidder, price >= 1000000) AS rank3_bidders, 
  count_distinct(auction) AS total_auctions, 
  count_distinct_if(auction, price < 10000) AS rank1_auctions, 
  count_distinct_if(auction, (price >= 10000) AND (price < 1000000)) AS rank2_auctions, 
  count_distinct_if(auction, price >= 1000000) AS rank3_auctions
FROM
  bid
GROUP BY
  day
SETTINGS
  seek_to = 'earliest'