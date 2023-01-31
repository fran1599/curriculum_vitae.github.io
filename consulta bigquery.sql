WITH
  users AS (
  SELECT
    DISTINCT l.uid,
    FIRST_VALUE(l.raw_data) OVER (PARTITION BY l.uid ORDER BY l.created_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS logData,
    FIRST_VALUE(l.created_at) OVER (PARTITION BY l.uid ORDER BY l.created_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS created_at
  FROM
    `application.user_logs` l
  WHERE
    JSON_EXTRACT_SCALAR( l.raw_data,
      '$.user_snapshot.group' ) = 'Cba_Gob'
    OR ( JSON_EXTRACT_SCALAR( l.raw_data,
        '$.subtype' ) = "kit_activation"
      AND JSON_EXTRACT_SCALAR( l.raw_data,
        '$.data.group' ) = 'Cba_Gob' )
  GROUP BY
    l.uid,
    l.raw_data,
    l.created_at ),
  lastSmokeRecord AS (
    SELECT
    DISTINCT sr.uid,
    FIRST_VALUE(sr.day) OVER (PARTITION BY sr.uid ORDER BY sr.day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS day,
    FIRST_VALUE(sr.amount) OVER (PARTITION BY sr.uid ORDER BY sr.day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS amount
  FROM
    `application.smoke_records` sr
  GROUP BY
    sr.uid,
    sr.day,
    sr.amount
)
SELECT
  sr.uid AS `user_uid`,
  sr.day AS `smoke_day`,
  sr.amount AS `smoke_amount`,
  sr.updated_at AS `smoke_record_date`,
  lsr.amount as `last_smoke_record`,
  JSON_EXTRACT_SCALAR(u.logData,
    '$.user_snapshot.group') AS `user_group`
FROM
  `application.smoke_records` AS sr
RIGHT JOIN
  users AS u
ON
  u.uid = sr.uid
LEFT JOIN
  lastSmokeRecord as lsr
ON
  lsr.uid = sr.uid
WHERE
  sr.uid IS NOT NULL
  AND
  CAST(sr.day AS date) BETWEEN PARSE_DATE("%Y%m%d",
    @DS_START_DATE)
  AND PARSE_DATE("%Y%m%d",
    @DS_END_DATE)
ORDER BY sr.uid, sr.day DESC