with
        RECENCY as (
                select
                        T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
                        MAX(DATE('2024-09-01')) - MAX(DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) DAYS_SINCE_LAST_TX,
                        MAX(DATE('2024-09-01')) - MIN(DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) DAYS_SINCE_FIRST_TX
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
                        -- and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-09-01')
                        and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2024-08-31')
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
                        -- and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-09-01')
                        and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2024-08-31')
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
        ),
        PAYTYPE_MAIN as (
                select distinct
                        T1.COMPANY_ID,
                        T3.CARDUP_PAYMENT_PAYMENT_TYPE,
                        COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                                partition by
                                        T1.COMPANY_ID,
                                        T3.CARDUP_PAYMENT_PAYMENT_TYPE
                        ) / COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                                partition by
                                        T1.COMPANY_ID
                        ) as COUNT_PAYMENT_ID_PAYMENT_TYPE_PROP
                from
                        DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED T1
                        join ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T3 on T1.COMPANY_ID = T3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                where
                        T1.OWNER = 'Unmanaged'
                        and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
                        AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
                        and CARDUP_PAYMENT_CU_LOCALE_ID = 1
        ),
        PAYTYPE as (
                select
                        COMPANY_ID,
                        "'Condo & MCST fees'" as PROPORTION_PAYMENT_ID_CONDO_MCST_FEES,
                        "'Payroll'" as PROPORTION_PAYMENT_ID_PAYROLL,
                        "'Property Tax'" as PROPORTION_PAYMENT_ID_PROPERTY_TAX,
                        "'Helper Salary'" as PROPORTION_PAYMENT_ID_HELPER_SALARY,
                        "'Income Tax'" as PROPORTION_PAYMENT_ID_INCOME_TAX,
                        "'Rent'" as PROPORTION_PAYMENT_ID_RENT,
                        "'Car Loan'" as PROPORTION_PAYMENT_ID_CAR_LOAN,
                        "'Mortgage'" as PROPORTION_PAYMENT_ID_MORTGAGE,
                        "'Education'" as PROPORTION_PAYMENT_ID_EDUCATION,
                        "'Corporate Tax'" as PROPORTION_PAYMENT_ID_CORPORATE_TAX,
                        "'Supplier'" as PROPORTION_PAYMENT_ID_SUPPLIER,
                        "'Renovation'" as PROPORTION_PAYMENT_ID_RENOVATION,
                        "'Electricity'" as PROPORTION_PAYMENT_ID_ELECTRICITY,
                        "'Insurance'" as PROPORTION_PAYMENT_ID_INSURANCE,
                        "'Stamp Duty'" as PROPORTION_PAYMENT_ID_STAMP_DUTY,
                        "'GST'" as PROPORTION_PAYMENT_ID_GST,
                        "'Misc'" as PROPORTION_PAYMENT_ID_MISC,
                        "'Parking'" as PROPORTION_PAYMENT_ID_PARKING
                from
                        PAYTYPE_MAIN PIVOT (
                                SUM(COUNT_PAYMENT_ID_PAYMENT_TYPE_PROP) FOR CARDUP_PAYMENT_PAYMENT_TYPE IN (
                                        'Condo & MCST fees',
                                        'Payroll',
                                        'Property Tax',
                                        'Helper Salary',
                                        'Income Tax',
                                        'Rent',
                                        'Car Loan',
                                        'Mortgage',
                                        'Education',
                                        'Corporate Tax',
                                        'Supplier',
                                        'Renovation',
                                        'Electricity',
                                        'Insurance',
                                        'Stamp Duty',
                                        'GST',
                                        'Misc',
                                        'Parking'
                                )
                        )
        ),
        CLUSTX as (
                select
                        COMPANY_ID,
                        CLUSTER_KMEANS CLUSTER --using kmeans
                from
                        DEV.SBOX_SHILTON.TEST_CLUSTERING_RESULTS
        ),
        CLUSTX2 as (
                select
                        COMPANY_ID,
                        CLUSTER_KMEANS_HVCONLY CLUSTER --using kmeans
                from
                        DEV.SBOX_SHILTON.TEST_CLUSTERING_RESULTS_HVCONLY
        )
select distinct
        COMPANY_ID,
        RECENCY.DAYS_SINCE_FIRST_TX,
        RECENCY.DAYS_SINCE_LAST_TX,
        FREQ_MON.AVG_MONTHLY_PAYMENT_COUNT,
        FREQ_MON.AVG_MONTHLY_PAYMENTS,
        RECURRING.PAYMENT_SCHEDULE_ID_PROPORTION_RECURRING,
        IFF(
                PAYTYPE.PROPORTION_PAYMENT_ID_CONDO_MCST_FEES is null,
                0,
                PAYTYPE.PROPORTION_PAYMENT_ID_CONDO_MCST_FEES
        ) PROPORTION_PAYMENT_ID_CONDO_MCST_FEES,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_PAYROLL is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_PAYROLL) PROPORTION_PAYMENT_ID_PAYROLL,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_PROPERTY_TAX is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_PROPERTY_TAX) PROPORTION_PAYMENT_ID_PROPERTY_TAX,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_HELPER_SALARY is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_HELPER_SALARY) PROPORTION_PAYMENT_ID_HELPER_SALARY,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_INCOME_TAX is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_INCOME_TAX) PROPORTION_PAYMENT_ID_INCOME_TAX,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_RENT is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_RENT) PROPORTION_PAYMENT_ID_RENT,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_CAR_LOAN is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_CAR_LOAN) PROPORTION_PAYMENT_ID_CAR_LOAN,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_MORTGAGE is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_MORTGAGE) PROPORTION_PAYMENT_ID_MORTGAGE,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_EDUCATION is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_EDUCATION) PROPORTION_PAYMENT_ID_EDUCATION,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_CORPORATE_TAX is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_CORPORATE_TAX) PROPORTION_PAYMENT_ID_CORPORATE_TAX,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_SUPPLIER is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_SUPPLIER) PROPORTION_PAYMENT_ID_SUPPLIER,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_RENOVATION is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_RENOVATION) PROPORTION_PAYMENT_ID_RENOVATION,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_ELECTRICITY is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_ELECTRICITY) PROPORTION_PAYMENT_ID_ELECTRICITY,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_INSURANCE is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_INSURANCE) PROPORTION_PAYMENT_ID_INSURANCE,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_STAMP_DUTY is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_STAMP_DUTY) PROPORTION_PAYMENT_ID_STAMP_DUTY,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_GST is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_GST) PROPORTION_PAYMENT_ID_GST,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_MISC is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_MISC) PROPORTION_PAYMENT_ID_MISC,
        IFF(PAYTYPE.PROPORTION_PAYMENT_ID_PARKING is null, 0, PAYTYPE.PROPORTION_PAYMENT_ID_PARKING) PROPORTION_PAYMENT_ID_PARKING,
        FREQ_MON.FIRST_ACTIVE_MONTH_P12M,
        FREQ_MON.LAST_ACTIVE_MONTH_P12M,
        FREQ_MON.TOTAL_PAYMENTS,
        FREQ_MON.TOTAL_PAYMENT_COUNT,
        case
                when CLUSTX.CLUSTER is not null then CLUSTX.CLUSTER
                when RECENCY.DAYS_SINCE_LAST_TX < 365 then 90
                when RECENCY.DAYS_SINCE_LAST_TX > 364 then 300
                else null
        end as CLUSTER,
        case
                when CLUSTX2.CLUSTER is not null then CLUSTX2.CLUSTER
                else 99
        end as CLUSTER_HVC
from
        DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED MAIN_UNMANAGED
        join RECENCY using (COMPANY_ID)
        join FREQ_MON using (COMPANY_ID)
        join RECURRING using (COMPANY_ID)
        join PAYTYPE using (COMPANY_ID)
        left join CLUSTX using (COMPANY_ID)
        left join CLUSTX2 using (COMPANY_ID)
        --  join (select distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        --        from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        --        where date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2024-06-01')) t3
        --       on t3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = main_unmanaged.company_id
where
        OWNER = 'Unmanaged';