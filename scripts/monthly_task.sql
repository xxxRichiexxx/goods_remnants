DELETE FROM sttgaz.dm_isc_goods_remnants
WHERE load_date = '{next_execution_date}';

INSERT INTO sttgaz.dm_isc_goods_remnants
SELECT *
FROM sttgaz.stage_isc_goods_remnants
WHERE load_date = '{next_execution_date}';