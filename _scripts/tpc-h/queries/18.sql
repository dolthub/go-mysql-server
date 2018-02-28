select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    CUSTOMER,
    ORDERS,
    LINEITEM
where
    o_orderkey in (select
                    l_orderkey
                from
                    LINEITEM
                group by
                    l_orderkey
                having
                    sum(l_quantity) > 314
                ) and
    c_custkey = o_custkey and
    o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
