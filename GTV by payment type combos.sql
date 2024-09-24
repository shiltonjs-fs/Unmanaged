
with freq_mon_main as (select CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                            , date_trunc(month, date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) month_transaction
                            , owner
                            , cardup_payment_payment_type paytype
                            , sum(cardup_payment_usd_amt)                               sum_monthly_payments
                            , count(distinct DWH_CARDUP_PAYMENT_ID)                     count_monthly_payments
                       from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t1
                                join (select distinct company_id, owner
                                      from dev.sbox_shilton.cardup_user_managed_unmanaged) t2
                                     on t2.company_id = t1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                       WHERE CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
                         AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
                         and CARDUP_PAYMENT_CU_LOCALE_ID = 1
                         and owner = 'Unmanaged'
                         and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-09-01')
                         and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= date('2024-08-31')
                       group by 1, 2, 3, 4),
     freq_mon_1 as (select CARDUP_PAYMENT_CUSTOMER_COMPANY_ID company_id
                         , paytype
                         , sum(sum_monthly_payments)          total_payments
                         , sum(count_monthly_payments)        total_payment_count
                    from freq_mon_main
                    group by 1, 2)
    select 
    company_id 
    , max(case when paytype='Condo & MCST fees' then total_payments else 0 end) as paytype_condo_mcst_fees
    , max(case when paytype='Payroll' then total_payments else 0 end) as paytype_payroll
    , max(case when paytype='Property Tax' then total_payments else 0 end) as paytype_property_tax
    , max(case when paytype='Helper Salary' then total_payments else 0 end) as paytype_helper_salary
    , max(case when paytype='Income Tax' then total_payments else 0 end) as paytype_income_tax
    , max(case when paytype='Rent' then total_payments else 0 end) as paytype_rent
    , max(case when paytype='Car Loan' then total_payments else 0 end) as paytype_car_loan
    , max(case when paytype='Mortgage' then total_payments else 0 end) as paytype_mortgage
    , max(case when paytype='Education' then total_payments else 0 end) as paytype_education
    , max(case when paytype='Corporate Tax' then total_payments else 0 end) as paytype_corporate_tax
    , max(case when paytype='Supplier' then total_payments else 0 end) as paytype_supplier
    , max(case when paytype='Renovation' then total_payments else 0 end) as paytype_renovation
    , max(case when paytype='Electricity' then total_payments else 0 end) as paytype_electricity
    , max(case when paytype='Insurance' then total_payments else 0 end) as paytype_insurance
    , max(case when paytype='Stamp Duty' then total_payments else 0 end) as paytype_stamp_duty
    , max(case when paytype='GST' then total_payments else 0 end) as paytype_gst
    , max(case when paytype='Misc' then total_payments else 0 end) as paytype_misc
    , max(case when paytype='Parking' then total_payments else 0 end) as paytype_parking
    , sum(total_payments) sum_total_payments
    , sum(total_payment_count) sum_total_payment_count
    from freq_mon_1 t1 
    join (select distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
               from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
               where date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2024-06-01')) t3
              on t3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = t1.company_id
    group by 1;