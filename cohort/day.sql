CREATE TABLE day (
                             day_id          DATE PRIMARY KEY,
                             day_name        VARCHAR(255),
                             week_id         VARCHAR(255),
                             week_name       VARCHAR(255),
                             month_id        VARCHAR(255),
                             month_name      VARCHAR(255)
);

INSERT INTO day
  SELECT
    to_char(d, 'YYYY-MM-DD')::DATE             AS day_id,
    to_char(d, 'Dy, Mon DD YYYY')                 AS day_name,
    to_char(d, 'IYYY-IW')                AS week_id,
    to_char(d, 'IYYY "CW" IW')                    AS week_name,
    to_char(d, 'YYYY-MM')               AS month_id,
    to_char(d, 'YYYY Mon')                        AS month_name
  FROM generate_series('2013-10-01 00:00:00' :: TIMESTAMP, (now() AT TIME ZONE 'Asia/Hong_Kong' )  :: TIMESTAMP, '1 day') AS d
;
