CREATE STREAM kafka
(
  event_type int,
  person tuple (
    id int64,
    name string,
    emailAddress string,
    creditCard string,
    city string,
    state string,
    date_time datetime64,
    extra string
  ),
  auction tuple (
    id int64,
    itemName string,
    description string,
    initialBid int64,
    reserve int64,
    date_time datetime64,
    expires  datetime64,
    seller int64,
    category int64,
    extra string
  ),
  bid tuple (
    auction  int64,
    bidder  int64,
    price  int64,
    channel  string,
    url  string,
    date_time  datetime64,
    extra  string
  )
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-events'

CREATE STREAM person
(
  id int64,
  name string,
  emailAddress string,
  creditCard string,
  city string,
  state string,
  date_time datetime64,
  extra string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person'