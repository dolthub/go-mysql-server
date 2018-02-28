select 
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    PART,
    SUPPLIER,
    PARTSUPP,
    NATION,
    REGION
where
    p_partkey = ps_partkey and
    s_suppkey = ps_suppkey and
    p_size = 30 and
    p_type regexp '*STEEL' and
    s_nationkey = n_nationkey and
    n_regionkey = r_regionkey and
    r_name = 'ASIA' and
    ps_supplycost = (select
                        min(ps_supplycost)
                    from
                        PARTSUPP,
                        SUPPLIER,
                        NATION,
                        REGION
                    where
                        p_partkey = ps_partkey and
                        s_suppkey = ps_suppkey and
                        s_nationkey = n_nationkey and
                        n_regionkey = r_regionkey and r_name = 'ASIA'
                    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
