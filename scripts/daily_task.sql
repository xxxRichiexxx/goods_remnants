BEGIN TRANSACTION;

INSERT INTO sttgaz.dm_isc_goods_remnants
WITH sq AS(
    SELECT VIN
    FROM sttgaz.dm_isc_goods_remnants
    WHERE DATE_TRUNC('MONTH', load_date) = DATE_TRUNC('MONTH', '{{next_execution_date}}')
)
SELECT *
FROM sttgaz.stage_isc_goods_remnants
WHERE load_date = '{{next_execution_date}}'
    AND VIN NOT IN (SELECT * FROM sq);

DELETE FROM sttgaz.dm_isc_goods_remnants
WHERE DATE_TRUNC('MONTH', load_date) = DATE_TRUNC('MONTH', '{{next_execution_date}}')
    AND VIN NOT IN (SELECT VIN
                    FROM sttgaz.stage_isc_goods_remnants
                    WHERE load_date = '{{next_execution_date}}');

COMMIT TRANSACTION;