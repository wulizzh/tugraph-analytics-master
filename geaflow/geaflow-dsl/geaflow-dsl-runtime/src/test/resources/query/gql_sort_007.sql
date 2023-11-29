CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  d_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	d.id
FROM (
  MATCH (a where id in (4))-(b)
  MATCH (b)-(c)-(d) order by c.id DESC, d.id, b.id
  RETURN a, b, d
)
;

