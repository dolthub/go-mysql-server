select
    o_orderpriority,
    count(*) as order_count
from
    ORDERS
where
    o_orderdate >= date '1995-01-01' and
    o_orderdate < date '1995-01-01' + interval '3' month and
    exists (select
                *
            from
                LINEITEM
            where
                l_orderkey = o_orderkey and
                l_commitdate < l_receiptdate
            )
group by
    o_orderpriority
order by
    o_orderpriority;
