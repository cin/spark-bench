drop table if exists inventory_text;
create table inventory_text
(
    inv_date_sk               int,
    inv_item_sk               int,
    inv_warehouse_sk          int,
    inv_quantity_on_hand      bigint
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/inventory")
;
drop table if exists inventory;
create table inventory 
using parquet
${PARTITIONEDBY_STATEMENT}
as (select * from inventory_text DISTRIBUTE BY inv_date_sk)
;
drop table if exists inventory_text;
