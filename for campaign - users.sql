with
    RECENCY as (
        select
            T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
            MAX(DATE('2025-03-01')) - MAX(DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) DAYS_SINCE_LAST_TX,
            MAX(DATE('2025-03-01')) - MIN(DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) DAYS_SINCE_FIRST_TX
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            join (
                select
                    COMPANY_ID
                from
                    DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED
                where
                    OWNER = 'Unmanaged'
            ) T2 on T2.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            -- and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-12-01')
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2025-02-28')
        group by
            1
    ),
    PAY_GST as (
        select
            T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
            MAX(
                case
                    when T1.CARDUP_PAYMENT_PAYMENT_TYPE = 'GST' then 1
                    else 0
                end
            ) as PAYTYPE_GST
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
        where
            true
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2025-02-28')
            and CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
        group by
            1
    )
select distinct
    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
    CONCAT('CU', LPAD(T5.user_id, 8, 0)) ACCOUNT_NUMBER,
    case
        when T2.CLUSTER_KMEANS is not null then T2.CLUSTER_KMEANS
        when T4.DAYS_SINCE_LAST_TX > 364 then 300
        when T4.DAYS_SINCE_LAST_TX > 89 then 90
        else null
    end as CLUSTER_KMEANS,
    T5.*
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
    join DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED T3 on T3.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    join RECENCY T4 on T4.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    join PAY_GST on PAY_GST.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    left join DEV.SBOX_SHILTON.TEST_CLUSTERING_RESULTS T2 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.COMPANY_ID
    left join CBM.CARDUP_DB_REPORTING.USER_DATA T5 on T5.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
where
    true
    -- and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2023-12-01')
    and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2025-02-28')
    and T3.OWNER = 'Unmanaged'
    AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
    and T1.CARDUP_PAYMENT_CU_LOCALE_ID = 1
    and T5.CU_LOCALE_ID = 1
    and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding');