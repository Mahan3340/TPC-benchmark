BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24
LIMIT 1;
COMMIT;

l_temp = lineitem.filter("l_shipdate >= '1994-01-01' ").filter("l_shipdate < '1995-01-01' ").filter('l_discount >= 0.05').filter('l_discount <= 0.07').filter('l_quantity < 24')
res = l_temp.agg(F.sum(l_temp.L_EXTENDEDPRICE *(l_temp.L_DISCOUNT)).alias("revenue"))