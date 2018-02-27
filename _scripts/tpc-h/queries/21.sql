select
    s_name,
    count(*) as numwait
from
    SUPPLIER,
    LINEITEM l1,
    ORDERS,
    NATION
where
    s_suppkey = l1.l_suppkey and
    o_orderkey = l1.l_orderkey and
    o_orderstatus = 'F' and
    l1.l_receiptdate > l1.l_commitdate and
    (select
        count(*)
    from
        LINEITEM l2
    where
        l2.l_orderkey = l1.l_orderkey and
        l2.l_suppkey <> l1.l_suppkey
    ) > 0 and
    (select
        count(*)
    from
        LINEITEM l3
    where
        l3.l_orderkey = l1.l_orderkey and
        l3.l_suppkey <> l1.l_suppkey and
        l3.l_receiptdate > l3.l_commitdate
    ) = 0 and
    s_nationkey = n_nationkey and
    n_name = 'EGYPT'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;
