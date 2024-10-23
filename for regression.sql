select
    T1.COMPANY_ID,
    T1.INDUSTRY,
    T1.VERIFICATION_METHOD,
    MAX(
        case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'GST' then 1
            else 0
        end
    ) as TRANSACTED_GST_BEFORE,
    SUM(T3.CARDUP_PAYMENT_USD_AMT) TOTAL_TRANSACTIONS_VALUE,
    SUM(
        case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Supplier' then T3.CARDUP_PAYMENT_USD_AMT
            else null
        end
    ) TOTAL_TRANSACTIONS_VALUE_SUPPLIER,
    SUM(
        case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Rent' then T3.CARDUP_PAYMENT_USD_AMT
            else null
        end
    ) TOTAL_TRANSACTIONS_VALUE_RENT,
    SUM(
        case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Payroll' then T3.CARDUP_PAYMENT_USD_AMT
            else null
        end
    ) TOTAL_TRANSACTIONS_VALUE_PAYROLL,
    COUNT(distinct T3.DWH_CARDUP_PAYMENT_ID) TOTAL_TRANSACTIONS_COUNT,
    COUNT(
        distinct case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Supplier' then T3.DWH_CARDUP_PAYMENT_ID
            else null
        end
    ) TOTAL_TRANSACTIONS_COUNT_SUPPLIER,
    COUNT(
        distinct case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Rent' then T3.DWH_CARDUP_PAYMENT_ID
            else null
        end
    ) TOTAL_TRANSACTIONS_COUNT_RENT,
    COUNT(
        distinct case
            when T3.CARDUP_PAYMENT_PAYMENT_TYPE = 'Payroll' then T3.DWH_CARDUP_PAYMENT_ID
            else null
        end
    ) TOTAL_TRANSACTIONS_COUNT_PAYROLL,
from
    CBM.CARDUP_DB_REPORTING.COMPANY_DATA T1
    join DEV.SBOX_SHILTON.CARDUP_USER_MANAGED_UNMANAGED T2 using (COMPANY_ID)
    join ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T3 on T1.COMPANY_ID = T3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
where
    OWNER = 'Unmanaged'
group by
    1,
    2,
    3;