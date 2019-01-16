BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '108' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
LIMIT 1;
COMMIT;

SPARK-QUERY:

res = lineitem.filter("l_shipdate <= '1998-09-02' ").groupBy('l_returnflag','l_linestatus').agg( F.sum('l_quantity').alias('sum_qty'), F.sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).alias('sum_disc_price'), F.sum('l_extendedprice').alias('sum_base_price'), F.sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)*(1+lineitem.L_TAX)).alias('sum_charge'), F.avg('l_quantity').alias('avg_qty'), F.avg('l_extendedprice').alias('avg_price'), F.avg('l_discount').alias('avg_disc')).sort('l_returnflag','l_linestatus')


CREATE INDEX C_CUSTKEY_IDX ON CUSTOMER(C_CUSTKEY);
CREATE INDEX C_NATIONKEY_IDX ON CUSTOMER(C_NATIONKEY);
CREATE INDEX C_ACCTBAL_IDX ON CUSTOMER(C_ACCTBAL);

CREATE INDEX L_ORDERKEY_IDX ON LINEITEM(L_ORDERKEY);
CREATE INDEX L_PARTKEY_IDX ON LINEITEM(L_PARTKEY);
CREATE INDEX L_SUPPKEY_IDX ON LINEITEM(L_SUPPKEY);
CREATE INDEX L_EXTENDEDPRICE_IDX ON LINEITEM(L_EXTENDEDPRICE);
CREATE INDEX L_QUANTITY_IDX ON LINEITEM(L_QUANTITY);
CREATE INDEX L_DISCOUNT_IDX ON LINEITEM(L_DISCOUNT);
CREATE INDEX L_TAX_IDX ON LINEITEM(L_TAX);

CREATE INDEX O_ORDERKEY_IDX ON ORDERS(O_ORDERKEY);
CREATE INDEX O_CUSTKEY_IDX ON ORDERS(O_CUSTKEY);
CREATE INDEX O_TOTALPRICE_IDX ON ORDERS(O_TOTALPRICE);
CREATE INDEX O_ORDERPRIORITY_IDX ON ORDERS(O_ORDERPRIORITY);
CREATE INDEX O_SHIPPRIORITY_IDX ON ORDERS(O_SHIPPRIORITY);

CREATE INDEX P_PARTKEY_IDX ON PART(P_PARTKEY);
CREATE INDEX P_SIZE_IDX ON PART(P_SIZE);
CREATE INDEX P_RETAILPRICE_IDX ON PART(P_RETAILPRICE);

CREATE INDEX PS_PARTKEY_IDX ON PARTSUPP(PS_PARTKEY);
CREATE INDEX PS_SUPPKEY_IDX ON PARTSUPP(PS_SUPPKEY);
CREATE INDEX PS_AVAILQTY_IDX ON PARTSUPP(PS_AVAILQTY);

CREATE INDEX S_SUPPKEY_IDX ON SUPPLIER(S_SUPPKEY);
CREATE INDEX S_NATIONKEY_IDX ON SUPPLIER(S_NATIONKEY);
CREATE INDEX S_ACCTBAL_IDX ON SUPPLIER(S_ACCTBAL);



