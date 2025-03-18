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
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2024-03-01')
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2025-02-28')
        group by
            1
    ),
    FREQ_MON_MAIN as (
        select
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            DATE_TRUNC(month, DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) MONTH_TRANSACTION,
            OWNER,
            SUM(CARDUP_PAYMENT_USD_AMT) SUM_MONTHLY_PAYMENTS,
            COUNT(distinct DWH_CARDUP_PAYMENT_ID) COUNT_MONTHLY_PAYMENTS
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            join (
                select distinct
                    COMPANY_ID,
                    OWNER
                from
                    DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED
            ) T2 on T2.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and OWNER = 'Unmanaged'
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2024-03-01')
            and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2025-02-28')
        group by
            1,
            2,
            3
    ),
    FREQ_MON_1 as (
        select
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
            MIN(MONTH_TRANSACTION) FIRST_ACTIVE_MONTH_P12M,
            MAX(MONTH_TRANSACTION) LAST_ACTIVE_MONTH_P12M,
            SUM(SUM_MONTHLY_PAYMENTS) TOTAL_PAYMENTS,
            SUM(COUNT_MONTHLY_PAYMENTS) TOTAL_PAYMENT_COUNT
        from
            FREQ_MON_MAIN
        group by
            1
    ),
    FREQ_MON as (
        select
            COMPANY_ID,
            FIRST_ACTIVE_MONTH_P12M,
            LAST_ACTIVE_MONTH_P12M,
            TOTAL_PAYMENTS,
            TOTAL_PAYMENT_COUNT,
            TOTAL_PAYMENTS / GREATEST(1, DATEDIFF(month, FIRST_ACTIVE_MONTH_P12M, LAST_ACTIVE_MONTH_P12M)) as AVG_MONTHLY_PAYMENTS,
            TOTAL_PAYMENT_COUNT / GREATEST(1, DATEDIFF(month, FIRST_ACTIVE_MONTH_P12M, LAST_ACTIVE_MONTH_P12M)) as AVG_MONTHLY_PAYMENT_COUNT
        from
            FREQ_MON_1
    ),
    RECURRING_MAIN as (
        select distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            CARDUP_PAYMENT_SCHEDULE_TYPE,
            COUNT(distinct CARDUP_PAYMENT_SCHEDULE_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) COUNT_PAYMENT_SCHEDULE_ID_ALL,
            COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) COUNT_PAYMENT_ID_ALL,
            COUNT(distinct CARDUP_PAYMENT_SCHEDULE_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_SCHEDULE_TYPE
            ) COUNT_PAYMENT_SCHEDULE_ID_BY_SCHEDULE_TYPE,
            COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_SCHEDULE_TYPE
            ) COUNT_PAYMENT_ID_BY_SCHEDULE_TYPE,
            COUNT(distinct CARDUP_PAYMENT_SCHEDULE_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_SCHEDULE_TYPE
            ) / COUNT(distinct CARDUP_PAYMENT_SCHEDULE_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) PAYMENT_SCHEDULE_ID_PROPORTION,
            COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_SCHEDULE_TYPE
            ) / COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) PAYMENT_ID_PROPORTION
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            join (
                select distinct
                    COMPANY_ID,
                    OWNER
                from
                    DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED
            ) T2 on T2.COMPANY_ID = T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and OWNER = 'Unmanaged'
    ),
    RECURRING as (
        select
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then COUNT_PAYMENT_SCHEDULE_ID_BY_SCHEDULE_TYPE
                    else 0
                end
            ) as COUNT_PAYMENT_SCHEDULE_ID_ONEOFF,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then COUNT_PAYMENT_SCHEDULE_ID_BY_SCHEDULE_TYPE
                    else 0
                end
            ) as COUNT_PAYMENT_SCHEDULE_ID_RECURRING,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then COUNT_PAYMENT_ID_BY_SCHEDULE_TYPE
                    else 0
                end
            ) as COUNT_PAYMENT_ID_ONEOFF,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then COUNT_PAYMENT_ID_BY_SCHEDULE_TYPE
                    else 0
                end
            ) as COUNT_PAYMENT_ID_RECURRING,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then PAYMENT_SCHEDULE_ID_PROPORTION
                    else 0
                end
            ) as PAYMENT_SCHEDULE_ID_PROPORTION_ONEOFF,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then PAYMENT_SCHEDULE_ID_PROPORTION
                    else 0
                end
            ) as PAYMENT_SCHEDULE_ID_PROPORTION_RECURRING,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then PAYMENT_ID_PROPORTION
                    else 0
                end
            ) as PAYMENT_ID_PROPORTION_ONEOFF,
            MAX(
                case
                    when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then PAYMENT_ID_PROPORTION
                    else 0
                end
            ) as PAYMENT_ID_PROPORTION_RECURRING
        from
            RECURRING_MAIN
        group by
            1
    )
select distinct
    COMPANY_ID,
    RECENCY.DAYS_SINCE_FIRST_TX,
    RECENCY.DAYS_SINCE_LAST_TX,
    FREQ_MON.AVG_MONTHLY_PAYMENT_COUNT,
    FREQ_MON.AVG_MONTHLY_PAYMENTS,
    RECURRING.PAYMENT_SCHEDULE_ID_PROPORTION_RECURRING
from
    DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED MAIN_UNMANAGED
    join RECENCY using (COMPANY_ID)
    join FREQ_MON using (COMPANY_ID)
    join RECURRING using (COMPANY_ID)
    join (
        select distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        where
            DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2024-10-01')
    ) T3 on T3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = MAIN_UNMANAGED.COMPANY_ID
where
    OWNER = 'Unmanaged';