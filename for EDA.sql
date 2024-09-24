
with recency as
         (select t1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID                                 company_id
               , max(date('2024-09-01')) - max(date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) days_since_last_tx
               , max(date('2024-09-01')) - min(date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) days_since_first_tx
          from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t1
                   join (select company_id
                         from dev.sbox_shilton.cardup_user_managed_unmanaged
                         where owner = 'Unmanaged') t2
                        on t2.company_id = t1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID

          WHERE CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            -- and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-09-01')
            and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= date('2024-08-31')
          group by 1),
     freq_mon_main as (select CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                            , date_trunc(month, date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS)) month_transaction
                            , owner
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
                         -- and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2023-09-01')
                         and date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) <= date('2024-08-31')
                       group by 1, 2, 3),
     freq_mon_1 as (select CARDUP_PAYMENT_CUSTOMER_COMPANY_ID company_id
                         , min(month_transaction)             first_active_month_p12m
                         , max(month_transaction)             last_active_month_p12m
                         , sum(sum_monthly_payments)          total_payments
                         , sum(count_monthly_payments)        total_payment_count
                    from freq_mon_main
                    group by 1),
     freq_mon as (select company_id
                       , first_active_month_p12m
                       , last_active_month_p12m
                       , total_payments
                       , total_payment_count
                       , total_payments / greatest(1,
                                                   datediff(month, first_active_month_p12m, last_active_month_p12m))      as avg_monthly_payments
                       , total_payment_count / greatest(1,
                                                        datediff(month, first_active_month_p12m, last_active_month_p12m)) as avg_monthly_payment_count
                  from freq_mon_1),
     recurring_main as (select distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                                      , CARDUP_PAYMENT_SCHEDULE_TYPE
                                      , count(distinct CARDUP_PAYMENT_SCHEDULE_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)                               count_payment_schedule_id_all
                                      , count(distinct DWH_CARDUP_PAYMENT_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)                               count_payment_id_all
                                      , count(distinct CARDUP_PAYMENT_SCHEDULE_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID, CARDUP_PAYMENT_SCHEDULE_TYPE) count_payment_schedule_id_by_schedule_type
                                      , count(distinct DWH_CARDUP_PAYMENT_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID, CARDUP_PAYMENT_SCHEDULE_TYPE) count_payment_id_by_schedule_type
                                      , count(distinct CARDUP_PAYMENT_SCHEDULE_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID, CARDUP_PAYMENT_SCHEDULE_TYPE) /
                                        count(distinct CARDUP_PAYMENT_SCHEDULE_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)                               payment_schedule_id_proportion
                                      , count(distinct DWH_CARDUP_PAYMENT_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID, CARDUP_PAYMENT_SCHEDULE_TYPE) /
                                        count(distinct DWH_CARDUP_PAYMENT_ID)
                                              over (partition by CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)                               payment_id_proportion
                        from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t1
                                 join (select distinct company_id, owner
                                       from dev.sbox_shilton.cardup_user_managed_unmanaged) t2
                                      on t2.company_id = t1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                        WHERE CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
                          AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
                          and CARDUP_PAYMENT_CU_LOCALE_ID = 1
                          and owner = 'Unmanaged'),
     recurring as (select CARDUP_PAYMENT_CUSTOMER_COMPANY_ID company_id
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off'
                                      then count_payment_schedule_id_by_schedule_type
                                  else 0 end) as             count_payment_schedule_id_oneoff
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring'
                                      then count_payment_schedule_id_by_schedule_type
                                  else 0 end) as             count_payment_schedule_id_recurring
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then count_payment_id_by_schedule_type
                                  else 0 end) as             count_payment_id_oneoff
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then count_payment_id_by_schedule_type
                                  else 0 end) as             count_payment_id_recurring
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then payment_schedule_id_proportion
                                  else 0 end) as             payment_schedule_id_proportion_oneoff
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then payment_schedule_id_proportion
                                  else 0 end) as             payment_schedule_id_proportion_recurring
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'one off' then payment_id_proportion
                                  else 0 end) as             payment_id_proportion_oneoff
                        , max(case
                                  when CARDUP_PAYMENT_SCHEDULE_TYPE = 'recurring' then payment_id_proportion
                                  else 0 end) as             payment_id_proportion_recurring
                   from recurring_main
                   group by 1),
                   paytype_main as (select distinct t1.COMPANY_ID
                                    , t3.CARDUP_PAYMENT_PAYMENT_TYPE
                                    , count(distinct DWH_CARDUP_PAYMENT_ID)
                                            over (partition by t1.COMPANY_ID, t3.CARDUP_PAYMENT_PAYMENT_TYPE) /
                                      count(distinct DWH_CARDUP_PAYMENT_ID)
                                            over (partition by t1.COMPANY_ID) as count_payment_id_payment_type_prop
                      from dev.sbox_shilton.cardup_user_managed_unmanaged t1
                               join ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t3
                                    on t1.COMPANY_ID = t3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                      where t1.owner = 'Unmanaged'
                        and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
                        AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
                        and CARDUP_PAYMENT_CU_LOCALE_ID = 1),
     paytype as (select COMPANY_ID
                         ,
                        "'Condo & MCST fees'" as proportion_payment_id_condo_mcst_fees,
                        "'Payroll'"           as proportion_payment_id_payroll,
                        "'Property Tax'"      as proportion_payment_id_property_tax,
                        "'Helper Salary'"     as proportion_payment_id_helper_salary,
                        "'Income Tax'"        as proportion_payment_id_income_tax,
                        "'Rent'"              as proportion_payment_id_rent,
                        "'Car Loan'"          as proportion_payment_id_car_loan,
                        "'Mortgage'"          as proportion_payment_id_mortgage,
                        "'Education'"         as proportion_payment_id_education,
                        "'Corporate Tax'"     as proportion_payment_id_corporate_tax,
                        "'Supplier'"          as proportion_payment_id_supplier,
                        "'Renovation'"        as proportion_payment_id_renovation,
                        "'Electricity'"       as proportion_payment_id_electricity,
                        "'Insurance'"         as proportion_payment_id_insurance,
                        "'Stamp Duty'"        as proportion_payment_id_stamp_duty,
                        "'GST'"               as proportion_payment_id_gst,
                        "'Misc'"              as proportion_payment_id_misc,
                        "'Parking'"           as proportion_payment_id_parking
                 from paytype_main pivot (SUM(count_payment_id_payment_type_prop) FOR CARDUP_PAYMENT_PAYMENT_TYPE IN ('Condo & MCST fees',
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
                     'Parking')))
select distinct company_id
              , recency.days_since_first_tx
              , recency.days_since_last_tx
              , freq_mon.avg_monthly_payment_count
              , freq_mon.avg_monthly_payments
              , recurring.payment_schedule_id_proportion_recurring
            --   , paytype.proportion_payment_id_condo_mcst_fees
            --   , paytype.proportion_payment_id_payroll
            --   , paytype.proportion_payment_id_property_tax
            --   , paytype.proportion_payment_id_helper_salary
            --   , paytype.proportion_payment_id_income_tax
            --   , paytype.proportion_payment_id_rent
            --   , paytype.proportion_payment_id_car_loan
            --   , paytype.proportion_payment_id_mortgage
            --   , paytype.proportion_payment_id_education
            --   , paytype.proportion_payment_id_corporate_tax
            --   , paytype.proportion_payment_id_supplier
            --   , paytype.proportion_payment_id_renovation
            --   , paytype.proportion_payment_id_electricity
            --   , paytype.proportion_payment_id_insurance
            --   , paytype.proportion_payment_id_stamp_duty
            --   , paytype.proportion_payment_id_gst
            --   , paytype.proportion_payment_id_misc
            --   , paytype.proportion_payment_id_parking
            --      , freq_mon.first_active_month_p12m
            --      , freq_mon.last_active_month_p12m
            --      , freq_mon.total_payments
            --      , freq_mon.total_payment_count
from dev.sbox_shilton.cardup_user_managed_unmanaged main_unmanaged
         join recency using (company_id)
         join freq_mon using (company_id)
         join recurring using (company_id)
        --  join paytype using (company_id)
        --  join (select distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        --        from ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        --        where date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS) >= date('2024-06-01')) t3
        --       on t3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = main_unmanaged.company_id
where owner = 'Unmanaged';