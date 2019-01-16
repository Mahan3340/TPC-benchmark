BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#54'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 8 and l_quantity <= 8 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#22'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 13 and l_quantity <= 13 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#51'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 22 and l_quantity <= 22 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
LIMIT 1;
COMMIT;



datetime.datetime.now().time()
containers1 = ["SM CASE","SM BOX","SM PACK","SM PKG"]
containers2 = ["MED BAG","MED BOX","MED PKG","MED PACK"]
containers3 = ["LG CASE","LG BOX","LG PACK","LG PKG"]
p_l = part.join(lineitem,part.P_PARTKEY == lineitem.L_PARTKEY).filter(("l_shipmode like '%AIR%' " or "l_shipmode like '%AIR REG%' ") and "l_shipinstruct like '%DELIVER IN PERSON%'")
p_l_2 = p_l.filter( ( (p_l.P_BRAND.like("%Brand#54%")) & (p_l.P_CONTAINER.isin(containers1)) & (p_l.L_QUANTITY >= 8) & (p_l.L_QUANTITY <= 18) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 10) ) | ((p_l.P_BRAND.like("%Brand#22%")) & (p_l.P_CONTAINER.isin(containers2)) & (p_l.L_QUANTITY >= 3) & (p_l.L_QUANTITY <= 23) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 5) ) | ((p_l.P_BRAND.like("%Brand#51%")) & (p_l.P_CONTAINER.isin(containers3)) & (p_l.L_QUANTITY >= 22) & (p_l.L_QUANTITY <= 32) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 15) ) ).select((p_l.L_EXTENDEDPRICE*(1-p_l.L_DISCOUNT)).alias("VOLUME")).agg(F.sum("VOLUME"))
p_l_2.show()
datetime.datetime.now().time()













