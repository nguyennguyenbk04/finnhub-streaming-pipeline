-- Create the gold schema pointing at the MinIO bucket location
CREATE SCHEMA IF NOT EXISTS delta.gold
WITH (location = 's3a://crypto-warehouse/gold/');

-- Register each Delta table produced by the gold notebook
CALL delta.system.register_table(
    schema_name => 'gold',
    table_name  => 'dim_asset',
    table_location => 's3a://crypto-warehouse/gold/dim_asset'
);

CALL delta.system.register_table(
    schema_name => 'gold',
    table_name  => 'dim_date',
    table_location => 's3a://crypto-warehouse/gold/dim_date'
);

CALL delta.system.register_table(
    schema_name => 'gold',
    table_name  => 'dim_time',
    table_location => 's3a://crypto-warehouse/gold/dim_time'
);

CALL delta.system.register_table(
    schema_name => 'gold',
    table_name  => 'fact_daily_ohlc',
    table_location => 's3a://crypto-warehouse/gold/fact_daily_ohlc'
);
