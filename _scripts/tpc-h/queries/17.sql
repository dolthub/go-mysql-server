select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    LINEITEM,
    PART
where
    p_partkey = l_partkey and
    p_brand = 'Brand#44' and
    p_container = 'WRAP PKG' and
    l_quantity < (select
                    0.2 * avg(l_quantity)
                from
                    LINEITEM
                where
                    l_partkey = p_partkey
                );
