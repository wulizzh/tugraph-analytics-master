CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	c.id
FROM (
  MATCH(a:person where a.id = 4)-[e:created]->(b), (a:person) <-[:knows]- (c)
  RETURN a, b, c
)
;

