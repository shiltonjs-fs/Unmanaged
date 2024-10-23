with
  FREQ_MON_MAIN as (
    select
      CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
      OWNER,
      CARDUP_PAYMENT_PAYMENT_TYPE PAYTYPE,
      CARDUP_PAYMENT_SCHEDULE_TYPE,
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
      and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2023-10-01')
      and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= DATE('2024-09-30')
    group by
      1,
      2,
      3,
      4
  ),
  FREQ_MON_1 as (
    select
      CARDUP_PAYMENT_CUSTOMER_COMPANY_ID COMPANY_ID,
      PAYTYPE,
      CARDUP_PAYMENT_SCHEDULE_TYPE,
      APPROX_PERCENTILE(SUM_MONTHLY_PAYMENTS, 0.5) TOTAL_PAYMENTS,
      APPROX_PERCENTILE(COUNT_MONTHLY_PAYMENTS, 0.5) TOTAL_PAYMENTS_COUNT
    from
      FREQ_MON_MAIN
    group by
      1,
      2,
      3
  )
select
  T1.COMPANY_ID,
  CARDUP_PAYMENT_SCHEDULE_TYPE,
  T4.CLUSTER,
  MAX(
    case
      when PAYTYPE = 'Condo & MCST fees' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_CONDO_MCST_FEES,
  MAX(
    case
      when PAYTYPE = 'Payroll' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_PAYROLL,
  MAX(
    case
      when PAYTYPE = 'Property Tax' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_PROPERTY_TAX,
  MAX(
    case
      when PAYTYPE = 'Helper Salary' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_HELPER_SALARY,
  MAX(
    case
      when PAYTYPE = 'Income Tax' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_INCOME_TAX,
  MAX(
    case
      when PAYTYPE = 'Rent' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_RENT,
  MAX(
    case
      when PAYTYPE = 'Car Loan' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_CAR_LOAN,
  MAX(
    case
      when PAYTYPE = 'Mortgage' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_MORTGAGE,
  MAX(
    case
      when PAYTYPE = 'Education' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_EDUCATION,
  MAX(
    case
      when PAYTYPE = 'Corporate Tax' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_CORPORATE_TAX,
  MAX(
    case
      when PAYTYPE = 'Supplier' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_SUPPLIER,
  MAX(
    case
      when PAYTYPE = 'Renovation' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_RENOVATION,
  MAX(
    case
      when PAYTYPE = 'Electricity' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_ELECTRICITY,
  MAX(
    case
      when PAYTYPE = 'Insurance' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_INSURANCE,
  MAX(
    case
      when PAYTYPE = 'Stamp Duty' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_STAMP_DUTY,
  MAX(
    case
      when PAYTYPE = 'GST' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_GST,
  MAX(
    case
      when PAYTYPE = 'Misc' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_MISC,
  MAX(
    case
      when PAYTYPE = 'Parking' then TOTAL_PAYMENTS
      else 0
    end
  ) as PAYTYPE_PARKING,
  SUM(TOTAL_PAYMENTS) SUM_TOTAL_PAYMENTS,
  SUM(TOTAL_PAYMENTS_COUNT) COUNT_TOTAL_PAYMENTS
from
  FREQ_MON_1 T1
  join (
    select distinct
      CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    from
      ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
    where
      true
      -- and DATE(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= DATE('2024-06-01')
  ) T3 on T3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T1.COMPANY_ID
  join (
    select
      COMPANY_ID,
      CLUSTER_KMEANS CLUSTER --using kmeans
    from
      DEV.SBOX_SHILTON.TEST_CLUSTERING_RESULTS
  ) T4 on T4.COMPANY_ID = T1.COMPANY_ID
group by
  1,
  2,
  3;