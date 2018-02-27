select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    CUSTOMER,
    ORDERS,
    LINEITEM
where
    c_mktsegment = 'AUTOMOBILE' and
    c_custkey = o_custkey and
    l_orderkey = o_orderkey and
    o_orderdate < date '1995-03-13' and
    l_shipdate > date '1995-03-13'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
