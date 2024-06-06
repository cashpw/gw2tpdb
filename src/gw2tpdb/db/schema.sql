-- Based on https://gitlab.com/Silvers_Gw2/Market_Data_Processer/-/blob/main/src/config/db_entities.ts?ref_type=heads

CREATE TABLE IF NOT EXISTS daily_history (
  id INTEGER NOT NULL,
  buy_delisted INTEGER,
  buy_listed INTEGER,
  buy_price_avg INTEGER,
  buy_price_max INTEGER,
  buy_price_min INTEGER,
  buy_price_stdev REAL,
  buy_quantity_avg INTEGER,
  buy_quantity_max INTEGER,
  buy_quantity_min INTEGER,
  buy_quantity_stdev REAL,
  buy_sold INTEGER,
  buy_value INTEGER,
  count INTEGER,
  sell_delisted INTEGER,
  sell_listed INTEGER,
  sell_price_avg INTEGER,
  sell_price_max INTEGER,
  sell_price_min INTEGER,
  sell_price_stdev REAL,
  sell_quantity_avg INTEGER,
  sell_quantity_max INTEGER,
  sell_quantity_min INTEGER,
  sell_quantity_stdev REAL,
  sell_sold INTEGER,
  sell_value INTEGER,
  utc_timestamp INTEGER NOT NULL,

  PRIMARY KEY (id, utc_timestamp)
);

CREATE TABLE IF NOT EXISTS hourly_history (
  id INTEGER NOT NULL,
  buy_delisted INTEGER,
  buy_listed INTEGER,
  buy_price_avg INTEGER,
  buy_price_max INTEGER,
  buy_price_min INTEGER,
  buy_price_stdev REAL,
  buy_quantity_avg INTEGER,
  buy_quantity_max INTEGER,
  buy_quantity_min INTEGER,
  buy_quantity_stdev REAL,
  buy_sold INTEGER,
  buy_value INTEGER,
  count INTEGER,
  sell_delisted INTEGER,
  sell_listed INTEGER,
  sell_price_avg INTEGER,
  sell_price_max INTEGER,
  sell_price_min INTEGER,
  sell_price_stdev REAL,
  sell_quantity_avg INTEGER,
  sell_quantity_max INTEGER,
  sell_quantity_min INTEGER,
  sell_quantity_stdev REAL,
  sell_sold INTEGER,
  sell_value INTEGER,
  utc_timestamp INTEGER NOT NULL,

  PRIMARY KEY (id, utc_timestamp)
);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER NOT NULL,
  name TEXT NOT NULL,

  PRIMARY KEY (id),
  UNIQUE(id)
);
