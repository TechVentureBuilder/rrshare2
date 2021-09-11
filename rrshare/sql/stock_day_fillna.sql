CREATE TABLE "public"."stock_day_fillna" (
  "trade_date" date  NOT NULL,
  "code" char(20) COLLATE "pg_catalog"."default" NOT NULL,
  "adj_factor" float8,
  "open" float8,
  "high" float8,
  "low" float8,
  "close" float8,
  "pre_close" float8,
  "pct_chg" float8,
  "vol" float8,
  "amount" float8,
  "avg" float8,
  "ts_code" char(20) NOT NULL,

  CONSTRAINT "stock_day_fillna_pkey" PRIMARY KEY ("trade_date", "code")
)
