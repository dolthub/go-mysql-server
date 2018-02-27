select
    c_count,
    count(*) as custdist
from
    (select
        c_custkey,
        count(o_orderkey) as c_count
    from
        CUSTOMER
    left outer join
        ORDERS
    on
        c_custkey = o_custkey and
        o_comment not regexp '*pending*deposits*'
    group by
        c_custkey
    ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
