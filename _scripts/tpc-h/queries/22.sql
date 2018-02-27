select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone, 1, 2) as cntrycode,
            c_acctbal
        from
            CUSTOMER
        where
            substring(c_phone, 1, 2) in ('20', '40', '22', '30', '39', '42', '21') and
            c_acctbal > (
                            select
                                avg(c_acctbal)
                            from
                                CUSTOMER
                            where
                                c_acctbal > 0.00 and
                                substring(c_phone, 1, 2) in ('20', '40', '22', '30', '39', '42', '21')
                        ) and
            (
                select
                    *
                from
                    ORDERS
                where
                    o_custkey = c_custkey
            ) = 0
    ) as custsale
group by
    cntrycode
order by
    cntrycode;
