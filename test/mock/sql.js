const sqlStrOne = `
CREATE TABLE dm_t_uba_xsjyh_events (
    distinct_id VARCHAR AS distinct_id,
    event VARCHAR AS event,
    od.time BIGINT AS oper_time,
    properties.password_type VARCHAR AS password_type,
    properties.source_page VARCHAR AS source_page,
    properties.is_success VARCHAR AS is_success,
    properties.is_valid_account VARCHAR AS is_valid_account,
    properties.fail_reason VARCHAR AS fail_reason,
    properties.first_cate_name VARCHAR AS first_cate_name,
    properties.second_cate_name VARCHAR AS second_cate_name,
    properties.filter VARCHAR AS ods_filter,
    properties.commodity_first_cate VARCHAR AS commodity_first_cate,
    properties.risk_assessment_level VARCHAR AS risk_assessment_level,
    properties.commodity_id VARCHAR AS commodity_id,
    properties.commodity_name VARCHAR AS commodity_name,
    properties.commodity_term VARCHAR AS commodity_term,
    properties.commodity_second_cate VARCHAR AS commodity_second_cate,
    properties.commodity_remain_credit VARCHAR AS commodity_remain_credit,
    properties.commodity_annual_rate VARCHAR AS commodity_annual_rate,
    properties.commodity_daily_increase DECIMAL AS commodity_daily_increase,
    properties.commodity_risk_level VARCHAR AS commodity_risk_level,
    properties.commodity_min_purchase_amount DECIMAL 
    AS commodity_min_purchase_amount,
    properties.first_title_name VARCHAR AS first_title_name,
    properties.second_title_name VARCHAR AS second_title_name,
    properties.commodity_term_type VARCHAR AS commodity_term_type,
    properties.commodity_payment_method VARCHAR AS commodity_payment_method,
    properties.commodity_return DECIMAL AS commodity_return,
    properties.investment_type VARCHAR AS investment_type,
    properties.amount VARCHAR AS amount,
    properties.quantity DECIMAL AS quantity,
    properties.commodity_order_id VARCHAR AS commodity_order_id,
    properties.recommender VARCHAR AS recommender,
    properties.payment_frequence VARCHAR AS payment_frequence,
    properties.payment_term VARCHAR AS payment_term,
    properties.redeem_frequence VARCHAR AS redeem_frequence,
    properties.redeem_term VARCHAR AS redeem_term,
    properties.expected_revenue DECIMAL AS expected_revenue,
    properties.end_type VARCHAR AS end_type,
    properties.profession VARCHAR AS profession,
    properties.province VARCHAR AS province,
    properties.city VARCHAR AS city,
    properties.district VARCHAR AS district,
    properties.residence_type VARCHAR AS residence_type,
    properties.marriage_status VARCHAR AS marriage_status,
    properties.personal_yearly_income DECIMAL AS personal_yearly_income,
    properties.family_yearly_income DECIMAL AS family_yearly_income,
    properties.has_social_security VARCHAR AS has_social_security,
    properties.auto_renewal VARCHAR AS auto_renewal,
    properties.health_information VARCHAR AS health_information,
    properties.bonus_type VARCHAR AS bonus_type,
    properties.gold_deposit_frequency VARCHAR AS gold_deposit_frequency,
    properties.contract_id VARCHAR AS contract_id,
    properties.payment_method VARCHAR AS payment_method,
    properties.contract_frequence VARCHAR AS contract_frequence,
    properties.contract_term DECIMAL AS contract_term,
    properties.start_date VARCHAR AS start_date,
    properties.end_date VARCHAR AS end_date,
    properties.investment_period DECIMAL AS investment_period,
    properties.actual_end_date VARCHAR AS actual_end_date,
    properties.success_period DECIMAL AS success_period,
    properties.contract_payment_type VARCHAR AS contract_payment_type,
    properties.commodity_id_list VARCHAR AS commodity_id_list,
    properties.commodity_name_list VARCHAR AS commodity_name_list,
    properties.page_name VARCHAR AS page_name,
    properties.available_quantity DECIMAL AS available_quantity,
    properties.buy_in_commodity_id VARCHAR AS buy_in_commodity_id,
    properties.buy_in_commodity_name VARCHAR AS buy_in_commodity_name,
    properties.buy_in_commodity_yearly_increase DECIMAL 
    AS buy_in_commodity_yearly_increase,
    properties.withdraw_method VARCHAR AS withdraw_method,
    properties.service_charge DECIMAL AS service_charge,
    properties.transfer_type VARCHAR AS transfer_type,
    properties.transfer_rate DECIMAL AS transfer_rate,
    properties.transfer_id VARCHAR AS transfer_id,
    properties.accumulated_earnings DECIMAL AS accumulated_earnings,
    properties.accumulated_earnings_rate DECIMAL AS accumulated_earnings_rate,
    properties.loan_name VARCHAR AS loan_name,
    properties.loan_type VARCHAR AS loan_type,
    properties.expected_credit DECIMAL AS expected_credit,
    properties.phone_num VARCHAR AS phone_num,
    properties.name VARCHAR AS name,
    properties.selected_city VARCHAR AS selected_city,
    properties.is_pmn_atv VARCHAR AS is_pmn_atv,
    properties.atv_type VARCHAR AS atv_type,
    properties.atv_id VARCHAR AS atv_id,
    properties.atv_name VARCHAR AS atv_name,
    properties.atv_start_date VARCHAR AS atv_start_date,
    properties.atv_end_date VARCHAR AS atv_end_date,
    properties.invitor_name VARCHAR AS invitor_name,
    properties.invitor_phone_num VARCHAR AS invitor_phone_num,
    properties.is_invited VARCHAR AS is_invited,
    properties.is_atv_eligible VARCHAR AS is_atv_eligible,
    properties.button_name VARCHAR AS button_name,
    properties.task_name VARCHAR AS task_name,
    properties.prize_id VARCHAR AS prize_id,
    properties.prize_name VARCHAR AS prize_name,
    properties.keyword VARCHAR AS keyword,
    properties.is_history VARCHAR AS is_history,
    properties.result_count DECIMAL AS result_count,
    properties.is_recommend VARCHAR AS is_recommend,
    properties.date VARCHAR AS ods_date,
    properties.current_version VARCHAR AS current_version,
    properties.current_city VARCHAR AS current_city,
    properties.imei_idfa VARCHAR AS imei_idfa,
    properties.is_login VARCHAR AS is_login,
    properties.$model VARCHAR AS ods_model,
    properties.$app_version VARCHAR AS ods_app_version,
    properties.$os VARCHAR AS ods_os,
    properties.$os_version VARCHAR AS ods_os_version,
    properties.platform_name VARCHAR AS platform_name,
    project VARCHAR AS project,
    user_id VARCHAR AS user_id
) WITH (
    type = 'kafka10',
    bootstrapServers = '10.102.34.64:9092,10.102.34.65:9092,10.102.34.66:9092',
    zookeeperQuorum = '10.102.34.64:2181,10.102.34.65:2181,10.102.34.66:2181',
    offsetReset = 'latest',
    topic = 'event_topic',
    timezone = 'Asia/Shanghai',
    topicIsPattern = 'false',
    parallelism = '1',
    groupId = 'cba_CBA_EVENT_SJYH_8b22b54f_event_topic_dm_t_uba_xsjyh_events'
);
`;
const sqlStrTwo = `
CREATE TABLE dm_t_pecust_event_current (
    uuid DECIMAL,
    cust_no VARCHAR,
    ecif_cust_num VARCHAR,
    tourist_flag INT,
    is_tourist INT,
    oper_type VARCHAR,
    oper_sub VARCHAR,
    oper_date VARCHAR,
    oper_time VARCHAR,
    password_type VARCHAR,
    source_page VARCHAR,
    if_succ VARCHAR,
    is_valid_account VARCHAR,
    fail_reason VARCHAR,
    ods_event_duration VARCHAR,
    first_cate_name VARCHAR,
    second_cate_name VARCHAR,
    filter VARCHAR,
    commodity_first_cate VARCHAR,
    risk_assessment_level VARCHAR,
    commodity_id VARCHAR,
    commodity_name VARCHAR,
    commodity_term VARCHAR,
    commodity_second_cate VARCHAR,
    commodity_remain_credit VARCHAR,
    commodity_annual_rate VARCHAR,
    commodity_daily_increase DECIMAL,
    commodity_risk_level VARCHAR,
    commodity_min_purchase_amount DECIMAL,
    first_title_name VARCHAR,
    second_title_name VARCHAR,
    commodity_term_type VARCHAR,
    commodity_payment_method VARCHAR,
    commodity_return DECIMAL,
    investment_type VARCHAR,
    amount DECIMAL,
    quantity DECIMAL,
    commodity_order_id VARCHAR,
    recommender VARCHAR,
    payment_frequence VARCHAR,
    payment_term VARCHAR,
    redeem_frequence VARCHAR,
    redeem_term VARCHAR,
    expected_revenue DECIMAL,
    end_type VARCHAR,
    profession VARCHAR,
    ods_province VARCHAR,
    city VARCHAR,
    district VARCHAR,
    residence_type VARCHAR,
    marriage_status VARCHAR,
    personal_yearly_income DECIMAL,
    family_yearly_income DECIMAL,
    has_social_security VARCHAR,
    auto_renewal VARCHAR,
    health_information VARCHAR,
    bonus_type VARCHAR,
    gold_deposit_frequency VARCHAR,
    contract_id VARCHAR,
    pay_shape VARCHAR,
    contract_frequence VARCHAR,
    contract_term DECIMAL,
    xsjyh_cf_start_date VARCHAR,
    xsjyh_cf_end_date VARCHAR,
    investment_period DECIMAL,
    actual_end_date VARCHAR,
    success_period DECIMAL,
    contract_payment_type VARCHAR,
    commodity_id_list VARCHAR,
    commodity_name_list VARCHAR,
    page VARCHAR,
    available_quantity DECIMAL,
    buy_in_commodity_id VARCHAR,
    buy_in_commodity_name VARCHAR,
    buy_in_commodity_yearly_increase DECIMAL,
    withdraw_method VARCHAR,
    service_charge DECIMAL,
    transfer_type VARCHAR,
    transfer_rate DECIMAL,
    transfer_id VARCHAR,
    accumulated_earnings DECIMAL,
    accumulated_earnings_rate DECIMAL,
    loan_name VARCHAR,
    loan_type VARCHAR,
    expected_credit DECIMAL,
    submit_cus_phone VARCHAR,
    submit_cus_name VARCHAR,
    city_selected VARCHAR,
    is_pmn_atv VARCHAR,
    campaign_type VARCHAR,
    actv_num VARCHAR,
    actv_name VARCHAR,
    atv_start_date VARCHAR,
    atv_end_date VARCHAR,
    invitor_name VARCHAR,
    invitor_phone_num VARCHAR,
    is_invited VARCHAR,
    is_atv_eligible VARCHAR,
    button_name VARCHAR,
    task_name VARCHAR,
    prize_id VARCHAR,
    prize_name VARCHAR,
    keyword VARCHAR,
    is_history VARCHAR,
    result_count DECIMAL,
    is_recommend VARCHAR,
    xsjyh_event_flag VARCHAR,
    ods_date VARCHAR,
    current_version VARCHAR,
    current_city VARCHAR,
    imei_idfa VARCHAR,
    is_login VARCHAR,
    ods_model VARCHAR,
    ods_app_version VARCHAR,
    ods_os VARCHAR,
    ods_os_version VARCHAR,
    platorm_name VARCHAR
) WITH (
    type = 'libra',
    url = 'jdbc:postgresql://10.102.0.10:25308/zybdb',
    userName = 'cba_it',
    password = 'Zybank_123',
    tableName = 'dm_t_pecust_event_current',
    parallelism = '1'
);
`;
const sqlStrThree = `
CREATE TABLE dm_t_event_dep_property (
    uuid DECIMAL,
    cust_no VARCHAR,
    ecif_cust_num VARCHAR,
    tourist_flag INT,
    is_tourist INT,
    mobile_tel VARCHAR,
    PRIMARY KEY (uuid),
    PERIOD FOR SYSTEM_TIME
) WITH (
    type = 'elasticsearch6',
    address = '10.102.0.32:24148,10.102.0.33:24148,10.102.0.34:24148',
    estype = 'dm_t_event_dep_property',
    cacheSize = '10000',
    cacheTTLMs = '60000',
    partitionedJoin = 'false',
    parallelism = '1'
);
`;
const sqlStrFour = `
CREATE TABLE dm_t_event_dep_property1 (
    uuid DECIMAL,
    cust_no VARCHAR,
    ecif_cust_num VARCHAR,
    tourist_flag INT,
    is_tourist INT,
    mobile_tel VARCHAR,
    PRIMARY KEY (uuid),
    PERIOD FOR SYSTEM_TIME
) WITH (
    type = 'elasticsearch6',
    address = '10.102.0.32:24148,10.102.0.33:24148,10.102.0.34:24148',
    estype = 'dm_t_event_dep_property',
    cacheSize = '10000',
    cacheTTLMs = '60000',
    partitionedJoin = 'false',
    parallelism = '1'
);
`;
const sqlStrFive = `
create view dm_t_xsjyh_events as
SELECT
    a.distinct_id as cust_no,
    a.event as oper_type,
    a.event as oper_sub,
    CAST(
        DATE_FORMAT(
            TIMESTAMPADD(hour, -8, long_to_time_udf(a.oper_time)),
            '%Y%m%d'
        ) AS VARCHAR
    ) AS oper_date,
    CAST(
        DATE_FORMAT(
            TIMESTAMPADD(hour, -8, long_to_time_udf(a.oper_time)),
            '%Y-%m-%d %H:%i:%s'
        ) AS VARCHAR
    ) AS oper_time,
    COALESCE(a.password_type, '其它') as password_type,
    a.source_page as source_page,
    case
        when a.is_success = 'true' then '1'
        when a.is_success = 'false' then '0'
        else coalesce(a.is_success, '0')
    end as if_succ,
    case
        when a.is_valid_account = 'true' then '1'
        when a.is_valid_account = 'false' then '0'
        else coalesce(a.is_valid_account, '0')
    end as is_valid_account,
    a.fail_reason as fail_reason,
    a.ods_event_duration as ods_event_duration,
    a.first_cate_name as first_cate_name,
    COALESCE(a.second_cate_name, '其它') as second_cate_name,
    COALESCE(a.ods_filter, '其它') as ods_filter,
    COALESCE(a.commodity_first_cate, '其它') as commodity_first_cate,
    CASE
        WHEN a.risk_assessment_level LIKE '%稳健型%' THEN '稳健型'
        WHEN a.risk_assessment_level LIKE '%平衡型%' THEN '平衡型'
        WHEN a.risk_assessment_level LIKE '%进取型%' THEN '进取型'
        WHEN a.risk_assessment_level LIKE '%谨慎型%' THEN '谨慎型'
        WHEN a.risk_assessment_level LIKE '%激进型%' THEN '激进型'
        ELSE risk_assessment_level
    END as risk_assessment_level,
    a.commodity_id as commodity_id,
    a.commodity_name as commodity_name,
    a.commodity_term as commodity_term,
    CASE
        WHEN a.commodity_first_cate = '理财' THEN CASE
            WHEN a.commodity_second_cate IN ('1102', '1303', '1401') THEN '定期+'
            WHEN a.commodity_second_cate = '1301' THEN '周期'
            WHEN a.commodity_second_cate = '1700' THEN '活期+'
            WHEN a.commodity_second_cate = '1306' THEN '如意宝'
            ELSE '其它'
        END
        ELSE COALESCE(commodity_second_cate, '其它')
    END as commodity_second_cate,
    a.commodity_remain_credit as commodity_remain_credit,
    a.commodity_annual_rate as commodity_annual_rate,
    a.commodity_daily_increase as commodity_daily_increase,
    a.commodity_risk_level as commodity_risk_level,
    a.commodity_min_purchase_amount as commodity_min_purchase_amount,
    COALESCE(a.first_title_name, '其它') as first_title_name,
    COALESCE(a.second_title_name, '其它') as second_title_name,
    a.commodity_term_type as commodity_term_type,
    a.commodity_payment_method as commodity_payment_method,
    a.commodity_return as commodity_return,
    COALESCE(a.investment_type, '其它') as investment_type,
    cast(a.amount as decimal) as amount,
    a.quantity as quantity,
    a.commodity_order_id as commodity_order_id,
    a.recommender as recommender,
    a.payment_frequence as payment_frequence,
    COALESCE(a.payment_term, '其它') as payment_term,
    a.redeem_frequence as redeem_frequence,
    a.redeem_term as redeem_term,
    a.expected_revenue as expected_revenue,
    a.end_type as end_type,
    a.profession as profession,
    a.province as ods_province,
    a.city as city,
    a.district as district,
    a.residence_type as residence_type,
    a.marriage_status as marriage_status,
    a.personal_yearly_income as personal_yearly_income,
    a.family_yearly_income as family_yearly_income,
    CASE
        WHEN a.has_social_security = '是' THEN '1'
        ELSE '0'
    END as has_social_security,
    CASE
        WHEN a.auto_renewal = '是' THEN '1'
        ELSE '0'
    END as auto_renewal,
    COALESCE(a.health_information, '0') as health_information,
    a.bonus_type as bonus_type,
    COALESCE(a.gold_deposit_frequency, '其它') as gold_deposit_frequency,
    a.contract_id as contract_id,
    a.payment_method as pay_shape,
    COALESCE(a.contract_frequence, '其它') as contract_frequence,
    a.contract_term as contract_term,
    a.start_date as xsjyh_cf_start_date,
    a.end_date as xsjyh_cf_end_date,
    a.investment_period as investment_period,
    a.actual_end_date as actual_end_date,
    a.success_period as success_period,
    COALESCE(a.contract_payment_type, '其它') as contract_payment_type,
    a.commodity_id_list as commodity_id_list,
    a.commodity_name_list as commodity_name_list,
    a.page_name as page,
    a.available_quantity as available_quantity,
    a.buy_in_commodity_id as buy_in_commodity_id,
    a.buy_in_commodity_name as buy_in_commodity_name,
    a.buy_in_commodity_yearly_increase as buy_in_commodity_yearly_increase,
    COALESCE(a.withdraw_method, '其它') as withdraw_method,
    a.service_charge as service_charge,
    COALESCE(a.transfer_type, '其它') as transfer_type,
    a.transfer_rate as transfer_rate,
    a.transfer_id as transfer_id,
    a.accumulated_earnings as accumulated_earnings,
    a.accumulated_earnings_rate as accumulated_earnings_rate,
    a.loan_name as loan_name,
    a.loan_type as loan_type,
    a.expected_credit as expected_credit,
    a.phone_num as submit_cus_phone,
    a.name as submit_cus_name,
    COALESCE(a.selected_city, '其它') as city_selected,
    a.is_pmn_atv as is_pmn_atv,
    a.atv_type as campaign_type,
    a.atv_id as actv_num,
    a.atv_name as actv_name,
    a.atv_start_date as atv_start_date,
    a.atv_end_date as atv_end_date,
    a.invitor_name as invitor_name,
    a.invitor_phone_num as invitor_phone_num,
    case
        when a.is_invited = 'true' then '1'
        when a.is_invited = 'false' then '0'
        else coalesce(a.is_invited, '0')
    end as is_invited,
    case
        when a.is_atv_eligible = 'true' then '1'
        when a.is_atv_eligible = 'false' then '0'
        else coalesce(a.is_atv_eligible, '0')
    end as is_atv_eligible,
    COALESCE(a.button_name, '其它') as button_name,
    COALESCE(a.task_name, '其它') as task_name,
    a.prize_id as prize_id,
    a.prize_name as prize_name,
    a.keyword as keyword,
    case
        when a.is_history = 'true' then '1'
        when a.is_history = 'false' then '0'
        else coalesce(a.is_history, '0')
    end as is_history,
    a.result_count as result_count,
    case
        when a.is_recommend = 'true' then '1'
        when a.is_recommend = 'false' then '0'
        else coalesce(a.is_recommend, '0')
    end as is_recommend,
    '1' as xsjyh_event_flag,
    a.ods_date as ods_date,
    case
        when a.current_version = '1'
        or a.current_version = '大众' THEN '大众版'
        when a.current_version = '2' THEN '财私版'
        when a.current_version = '3' THEN '芳华版'
        when a.current_version = '4'
        or a.current_version = '惠农版' THEN '家和版'
        else COALESCE(a.current_version, '无法获取版本信息')
    end as current_version,
    a.current_city as current_city,
    a.imei_idfa as imei_idfa,
    case
        when a.is_login = 'true' then '1'
        when a.is_login = 'false' then '0'
        else coalesce(a.is_login, '0')
    end as is_login,
    a.ods_model as ods_model,
    a.ods_app_version as ods_app_version,
    a.ods_os as ods_os,
    a.ods_os_version as ods_os_version,
    COALESCE(a.platform_name, '其它') as platorm_name,
    user_id as user_id
FROM
    dm_t_uba_xsjyh_events a
where
    project = 'testshoujiyinhang'
    and event in (
        'fin_commodity_list_view',
        'fin_enter_risk_assessment',
        'fin_risk_assessment_result',
        'fin_commodity_detail_view',
        'fin_commodity_buy_click',
        'fin_submit_order_result',
        'fin_cancel_order',
        'fin_commodity_buy_result',
        'fin_commodity_contract_click',
        'fin_commodity_contract_result',
        'fin_terminate_contract_click',
        'fin_terminate_contract_result',
        'fin_my_favorite_view',
        'fin_transfer_commodity_click',
        'fin_submit_commodity_transfer',
        'fin_transfer_commodity_result',
        'fin_withdraw_click',
        'fin_withdraw_result'
    );
`;
const sqlStrSix = `
create view dm_t_xsjyh_events as
SELECT
    a.distinct_id as cust_no,
    a.event as oper_type,
    a.event as oper_sub,
    CAST(
        DATE_FORMAT(
            TIMESTAMPADD(hour, -8, long_to_time_udf(a.oper_time)),
            '%Y%m%d'
        ) AS VARCHAR
    ) AS oper_date,
    CAST(
        DATE_FORMAT(
            TIMESTAMPADD(hour, -8, long_to_time_udf(a.oper_time)),
            '%Y-%m-%d %H:%i:%s'
        ) AS VARCHAR
    ) AS oper_time,
    COALESCE(a.password_type, '其它') as password_type,
    a.source_page as source_page,
    case
        when a.is_success = 'true' then '1'
        when a.is_success = 'false' then '0'
        else coalesce(a.is_success, '0')
    end as if_succ,
    case
        when a.is_valid_account = 'true' then '1'
        when a.is_valid_account = 'false' then '0'
        else coalesce(a.is_valid_account, '0')
    end as is_valid_account,
    a.fail_reason as fail_reason,
    a.ods_event_duration as ods_event_duration,
    a.first_cate_name as first_cate_name,
    COALESCE(a.second_cate_name, '其它') as second_cate_name,
    COALESCE(a.ods_filter, '其它') as ods_filter,
    COALESCE(a.commodity_first_cate, '其它') as commodity_first_cate,
    CASE
        WHEN a.risk_assessment_level LIKE '%稳健型%' THEN '稳健型'
        WHEN a.risk_assessment_level LIKE '%平衡型%' THEN '平衡型'
        WHEN a.risk_assessment_level LIKE '%进取型%' THEN '进取型'
        WHEN a.risk_assessment_level LIKE '%谨慎型%' THEN '谨慎型'
        WHEN a.risk_assessment_level LIKE '%激进型%' THEN '激进型'
        ELSE risk_assessment_level
    END as risk_assessment_level,
    a.commodity_id as commodity_id,
    a.commodity_name as commodity_name,
    a.commodity_term as commodity_term,
    CASE
        WHEN a.commodity_first_cate = '理财' THEN CASE
            WHEN a.commodity_second_cate IN ('1102', '1303', '1401') THEN '定期+'
            WHEN a.commodity_second_cate = '1301' THEN '周期'
            WHEN a.commodity_second_cate = '1700' THEN '活期+'
            WHEN a.commodity_second_cate = '1306' THEN '如意宝'
            ELSE '其它'
        END
        ELSE COALESCE(commodity_second_cate, '其它')
    END as commodity_second_cate,
    a.commodity_remain_credit as commodity_remain_credit,
    a.commodity_annual_rate as commodity_annual_rate,
    a.commodity_daily_increase as commodity_daily_increase,
    a.commodity_risk_level as commodity_risk_level,
    a.commodity_min_purchase_amount as commodity_min_purchase_amount,
    COALESCE(a.first_title_name, '其它') as first_title_name,
    COALESCE(a.second_title_name, '其它') as second_title_name,
    a.commodity_term_type as commodity_term_type,
    a.commodity_payment_method as commodity_payment_method,
    a.commodity_return as commodity_return,
    COALESCE(a.investment_type, '其它') as investment_type,
    cast(a.amount as decimal) as amount,
    a.quantity as quantity,
    a.commodity_order_id as commodity_order_id,
    a.recommender as recommender,
    a.payment_frequence as payment_frequence,
    COALESCE(a.payment_term, '其它') as payment_term,
    a.redeem_frequence as redeem_frequence,
    a.redeem_term as redeem_term,
    a.expected_revenue as expected_revenue,
    a.end_type as end_type,
    a.profession as profession,
    a.province as ods_province,
    a.city as city,
    a.district as district,
    a.residence_type as residence_type,
    a.marriage_status as marriage_status,
    a.personal_yearly_income as personal_yearly_income,
    a.family_yearly_income as family_yearly_income,
    CASE
        WHEN a.has_social_security = '是' THEN '1'
        ELSE '0'
    END as has_social_security,
    CASE
        WHEN a.auto_renewal = '是' THEN '1'
        ELSE '0'
    END as auto_renewal,
    COALESCE(a.health_information, '0') as health_information,
    a.bonus_type as bonus_type,
    COALESCE(a.gold_deposit_frequency, '其它') as gold_deposit_frequency,
    a.contract_id as contract_id,
    a.payment_method as pay_shape,
    COALESCE(a.contract_frequence, '其它') as contract_frequence,
    a.contract_term as contract_term,
    a.start_date as xsjyh_cf_start_date,
    a.end_date as xsjyh_cf_end_date,
    a.investment_period as investment_period,
    a.actual_end_date as actual_end_date,
    a.success_period as success_period,
    COALESCE(a.contract_payment_type, '其它') as contract_payment_type,
    a.commodity_id_list as commodity_id_list,
    a.commodity_name_list as commodity_name_list,
    a.page_name as page,
    a.available_quantity as available_quantity,
    a.buy_in_commodity_id as buy_in_commodity_id,
    a.buy_in_commodity_name as buy_in_commodity_name,
    a.buy_in_commodity_yearly_increase as buy_in_commodity_yearly_increase,
    COALESCE(a.withdraw_method, '其它') as withdraw_method,
    a.service_charge as service_charge,
    COALESCE(a.transfer_type, '其它') as transfer_type,
    a.transfer_rate as transfer_rate,
    a.transfer_id as transfer_id,
    a.accumulated_earnings as accumulated_earnings,
    a.accumulated_earnings_rate as accumulated_earnings_rate,
    a.loan_name as loan_name,
    a.loan_type as loan_type,
    a.expected_credit as expected_credit,
    a.phone_num as submit_cus_phone,
    a.name as submit_cus_name,
    COALESCE(a.selected_city, '其它') as city_selected,
    a.is_pmn_atv as is_pmn_atv,
    a.atv_type as campaign_type,
    a.atv_id as actv_num,
    a.atv_name as actv_name,
    a.atv_start_date as atv_start_date,
    a.atv_end_date as atv_end_date,
    a.invitor_name as invitor_name,
    a.invitor_phone_num as invitor_phone_num,
    case
        when a.is_invited = 'true' then '1'
        when a.is_invited = 'false' then '0'
        else coalesce(a.is_invited, '0')
    end as is_invited,
    case
        when a.is_atv_eligible = 'true' then '1'
        when a.is_atv_eligible = 'false' then '0'
        else coalesce(a.is_atv_eligible, '0')
    end as is_atv_eligible,
    COALESCE(a.button_name, '其它') as button_name,
    COALESCE(a.task_name, '其它') as task_name,
    a.prize_id as prize_id,
    a.prize_name as prize_name,
    a.keyword as keyword,
    case
        when a.is_history = 'true' then '1'
        when a.is_history = 'false' then '0'
        else coalesce(a.is_history, '0')
    end as is_history,
    a.result_count as result_count,
    case
        when a.is_recommend = 'true' then '1'
        when a.is_recommend = 'false' then '0'
        else coalesce(a.is_recommend, '0')
    end as is_recommend,
    '1' as xsjyh_event_flag,
    a.ods_date as ods_date,
    case
        when a.current_version = '1'
        or a.current_version = '大众' THEN '大众版'
        when a.current_version = '2' THEN '财私版'
        when a.current_version = '3' THEN '芳华版'
        when a.current_version = '4'
        or a.current_version = '惠农版' THEN '家和版'
        else COALESCE(a.current_version, '无法获取版本信息')
    end as current_version,
    a.current_city as current_city,
    a.imei_idfa as imei_idfa,
    case
        when a.is_login = 'true' then '1'
        when a.is_login = 'false' then '0'
        else coalesce(a.is_login, '0')
    end as is_login,
    a.ods_model as ods_model,
    a.ods_app_version as ods_app_version,
    a.ods_os as ods_os,
    a.ods_os_version as ods_os_version,
    COALESCE(a.platform_name, '其它') as platorm_name,
    user_id as user_id
FROM
    dm_t_uba_xsjyh_events a
where
    project = 'testshoujiyinhang'
    and event in (
        'fin_commodity_list_view',
        'fin_enter_risk_assessment',
        'fin_risk_assessment_result',
        'fin_commodity_detail_view',
        'fin_commodity_buy_click',
        'fin_submit_order_result',
        'fin_cancel_order',
        'fin_commodity_buy_result',
        'fin_commodity_contract_click',
        'fin_commodity_contract_result',
        'fin_terminate_contract_click',
        'fin_terminate_contract_result',
        'fin_my_favorite_view',
        'fin_transfer_commodity_click',
        'fin_submit_commodity_transfer',
        'fin_transfer_commodity_result',
        'fin_withdraw_click',
        'fin_withdraw_result'
    );
`;
const sqlStrSeven = `
INSERT INTO
    dm_t_pecust_event_current
SELECT
    b.uuid,
    b.cust_no AS cust_no,
    b.ecif_cust_num,
    b.tourist_flag,
    b.is_tourist,
    a.oper_type,
    a.oper_sub,
    a.oper_date,
    a.oper_time,
    a.password_type,
    a.source_page,
    a.if_succ,
    a.is_valid_account,
    a.fail_reason,
    a.ods_event_duration,
    a.first_cate_name,
    a.second_cate_name,
    a.ods_filter AS 'filter',
    a.commodity_first_cate,
    a.risk_assessment_level,
    a.commodity_id,
    a.commodity_name,
    a.commodity_term,
    a.commodity_second_cate,
    a.commodity_remain_credit,
    a.commodity_annual_rate,
    a.commodity_daily_increase,
    a.commodity_risk_level,
    a.commodity_min_purchase_amount,
    a.first_title_name,
    a.second_title_name,
    a.commodity_term_type,
    a.commodity_payment_method,
    a.commodity_return,
    a.investment_type,
    a.amount,
    a.quantity,
    a.commodity_order_id,
    a.recommender,
    a.payment_frequence,
    a.payment_term,
    a.redeem_frequence,
    a.redeem_term,
    a.expected_revenue,
    a.end_type,
    a.profession,
    a.ods_province,
    a.city,
    a.district,
    a.residence_type,
    a.marriage_status,
    a.personal_yearly_income,
    a.family_yearly_income,
    a.has_social_security,
    a.auto_renewal,
    a.health_information,
    a.bonus_type,
    a.gold_deposit_frequency,
    a.contract_id,
    a.pay_shape,
    a.contract_frequence,
    a.contract_term,
    a.xsjyh_cf_start_date,
    a.xsjyh_cf_end_date,
    a.investment_period,
    a.actual_end_date,
    a.success_period,
    a.contract_payment_type,
    a.commodity_id_list,
    a.commodity_name_list,
    a.page,
    a.available_quantity,
    a.buy_in_commodity_id,
    a.buy_in_commodity_name,
    a.buy_in_commodity_yearly_increase,
    a.withdraw_method,
    a.service_charge,
    a.transfer_type,
    a.transfer_rate,
    a.transfer_id,
    a.accumulated_earnings,
    a.accumulated_earnings_rate,
    a.loan_name,
    a.loan_type,
    a.expected_credit,
    a.submit_cus_phone,
    a.submit_cus_name,
    a.city_selected,
    a.is_pmn_atv,
    a.campaign_type,
    a.actv_num,
    a.actv_name,
    a.atv_start_date,
    a.atv_end_date,
    a.invitor_name,
    a.invitor_phone_num,
    a.is_invited,
    a.is_atv_eligible,
    a.button_name,
    a.task_name,
    a.prize_id,
    a.prize_name,
    a.keyword,
    a.is_history,
    a.result_count,
    a.is_recommend,
    '1' AS xsjyh_event_flag,
    a.ods_date,
    a.current_version,
    a.current_city,
    a.imei_idfa,
    a.is_login,
    a.ods_model,
    a.ods_app_version,
    a.ods_os,
    a.ods_os_version,
    a.platorm_name
FROM
    dm_t_xsjyh_events a
    INNER JOIN dm_t_event_dep_property1 b ON a.cust_no = b.cust_no
WHERE
    char_length(a.cust_no) = 10;
`;
const sqlStrEight = `
INSERT INTO
    dm_t_pecust_event_current
SELECT
    b.uuid,
    b.cust_no AS cust_no,
    b.ecif_cust_num,
    b.tourist_flag,
    b.is_tourist,
    a.oper_type,
    a.oper_sub,
    a.oper_date,
    a.oper_time,
    a.password_type,
    a.source_page,
    a.if_succ,
    a.is_valid_account,
    a.fail_reason,
    a.ods_event_duration,
    a.first_cate_name,
    a.second_cate_name,
    a.ods_filter AS 'filter',
    a.commodity_first_cate,
    a.risk_assessment_level,
    a.commodity_id,
    a.commodity_name,
    a.commodity_term,
    a.commodity_second_cate,
    a.commodity_remain_credit,
    a.commodity_annual_rate,
    a.commodity_daily_increase,
    a.commodity_risk_level,
    a.commodity_min_purchase_amount,
    a.first_title_name,
    a.second_title_name,
    a.commodity_term_type,
    a.commodity_payment_method,
    a.commodity_return,
    a.investment_type,
    a.amount,
    a.quantity,
    a.commodity_order_id,
    a.recommender,
    a.payment_frequence,
    a.payment_term,
    a.redeem_frequence,
    a.redeem_term,
    a.expected_revenue,
    a.end_type,
    a.profession,
    a.ods_province,
    a.city,
    a.district,
    a.residence_type,
    a.marriage_status,
    a.personal_yearly_income,
    a.family_yearly_income,
    a.has_social_security,
    a.auto_renewal,
    a.health_information,
    a.bonus_type,
    a.gold_deposit_frequency,
    a.contract_id,
    a.pay_shape,
    a.contract_frequence,
    a.contract_term,
    a.xsjyh_cf_start_date,
    a.xsjyh_cf_end_date,
    a.investment_period,
    a.actual_end_date,
    a.success_period,
    a.contract_payment_type,
    a.commodity_id_list,
    a.commodity_name_list,
    a.page,
    a.available_quantity,
    a.buy_in_commodity_id,
    a.buy_in_commodity_name,
    a.buy_in_commodity_yearly_increase,
    a.withdraw_method,
    a.service_charge,
    a.transfer_type,
    a.transfer_rate,
    a.transfer_id,
    a.accumulated_earnings,
    a.accumulated_earnings_rate,
    a.loan_name,
    a.loan_type,
    a.expected_credit,
    a.submit_cus_phone,
    a.submit_cus_name,
    a.city_selected,
    a.is_pmn_atv,
    a.campaign_type,
    a.actv_num,
    a.actv_name,
    a.atv_start_date,
    a.atv_end_date,
    a.invitor_name,
    a.invitor_phone_num,
    a.is_invited,
    a.is_atv_eligible,
    a.button_name,
    a.task_name,
    a.prize_id,
    a.prize_name,
    a.keyword,
    a.is_history,
    a.result_count,
    a.is_recommend,
    '2' AS xsjyh_event_flag,
    a.ods_date,
    a.current_version,
    a.current_city,
    a.imei_idfa,
    a.is_login,
    a.ods_model,
    a.ods_app_version,
    a.ods_os,
    a.ods_os_version,
    a.platorm_name
FROM
    dm_t_xsjyh_events a
    INNER JOIN f_uba_xsjyh_users1 c ON a.user_id = c.id
    INNER JOIN dm_t_event_dep_property2 b ON c.second_id = b.cust_no
WHERE
    char_length(a.cust_no) <> 10
    AND char_length(c.second_id) = 10;
`;

const allSqlStr = `
${sqlStrOne}
${sqlStrTwo}
${sqlStrThree}
${sqlStrFour}
${sqlStrFive}
${sqlStrSix}
${sqlStrSeven}
${sqlStrEight}
`;

export default {
    sqlStrOne, // - $ 符号后内容匹配不准确
    sqlStrTwo,
    sqlStrThree,
    sqlStrFour,
    sqlStrFive,
    sqlStrSix,
    sqlStrSeven,
    sqlStrEight,
    allSqlStr,
};
