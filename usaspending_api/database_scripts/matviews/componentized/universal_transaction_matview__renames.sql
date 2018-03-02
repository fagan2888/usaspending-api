--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;
ALTER INDEX IF EXISTS idx_a2023396__transaction_id RENAME TO idx_a2023396__transaction_id_old;
ALTER INDEX IF EXISTS idx_a2023396__action_date RENAME TO idx_a2023396__action_date_old;
ALTER INDEX IF EXISTS idx_a2023396__fiscal_year RENAME TO idx_a2023396__fiscal_year_old;
ALTER INDEX IF EXISTS idx_a2023396__type RENAME TO idx_a2023396__type_old;
ALTER INDEX IF EXISTS idx_a2023396__ordered_type RENAME TO idx_a2023396__ordered_type_old;
ALTER INDEX IF EXISTS idx_a2023396__action_type RENAME TO idx_a2023396__action_type_old;
ALTER INDEX IF EXISTS idx_a2023396__award_id RENAME TO idx_a2023396__award_id_old;
ALTER INDEX IF EXISTS idx_a2023396__award_category RENAME TO idx_a2023396__award_category_old;
ALTER INDEX IF EXISTS idx_a2023396__total_obligation RENAME TO idx_a2023396__total_obligation_old;
ALTER INDEX IF EXISTS idx_a2023396__ordered_total_obligation RENAME TO idx_a2023396__ordered_total_obligation_old;
ALTER INDEX IF EXISTS idx_a2023396__total_obl_bin RENAME TO idx_a2023396__total_obl_bin_old;
ALTER INDEX IF EXISTS idx_a2023396__total_subsidy_cost RENAME TO idx_a2023396__total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_a2023396__ordered_total_subsidy_cost RENAME TO idx_a2023396__ordered_total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_a2023396__pop_country_code RENAME TO idx_a2023396__pop_country_code_old;
ALTER INDEX IF EXISTS idx_a2023396__pop_state_code RENAME TO idx_a2023396__pop_state_code_old;
ALTER INDEX IF EXISTS idx_a2023396__pop_county_code RENAME TO idx_a2023396__pop_county_code_old;
ALTER INDEX IF EXISTS idx_a2023396__pop_zip5 RENAME TO idx_a2023396__pop_zip5_old;
ALTER INDEX IF EXISTS idx_a2023396__pop_congressional_code RENAME TO idx_a2023396__pop_congressional_code_old;
ALTER INDEX IF EXISTS idx_a2023396__gin_recipient_name RENAME TO idx_a2023396__gin_recipient_name_old;
ALTER INDEX IF EXISTS idx_a2023396__gin_recipient_unique_id RENAME TO idx_a2023396__gin_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_a2023396__gin_parent_recipient_unique_id RENAME TO idx_a2023396__gin_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_id RENAME TO idx_a2023396__recipient_id_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_name RENAME TO idx_a2023396__recipient_name_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_unique_id RENAME TO idx_a2023396__recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_a2023396__parent_recipient_unique_id RENAME TO idx_a2023396__parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_a2023396__awarding_agency_id RENAME TO idx_a2023396__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_a2023396__funding_agency_id RENAME TO idx_a2023396__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_a2023396__awarding_toptier_agency_name RENAME TO idx_a2023396__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_a2023396__awarding_subtier_agency_name RENAME TO idx_a2023396__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_a2023396__funding_toptier_agency_name RENAME TO idx_a2023396__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_a2023396__funding_subtier_agency_name RENAME TO idx_a2023396__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_location_country_code RENAME TO idx_a2023396__recipient_location_country_code_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_location_state_code RENAME TO idx_a2023396__recipient_location_state_code_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_location_county_code RENAME TO idx_a2023396__recipient_location_county_code_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_location_zip5 RENAME TO idx_a2023396__recipient_location_zip5_old;
ALTER INDEX IF EXISTS idx_a2023396__recipient_location_congressional_code RENAME TO idx_a2023396__recipient_location_congressional_code_old;
ALTER INDEX IF EXISTS idx_a2023396__cfda_multi RENAME TO idx_a2023396__cfda_multi_old;
ALTER INDEX IF EXISTS idx_a2023396__pulled_from RENAME TO idx_a2023396__pulled_from_old;
ALTER INDEX IF EXISTS idx_a2023396__type_of_contract_pricing RENAME TO idx_a2023396__type_of_contract_pricing_old;
ALTER INDEX IF EXISTS idx_a2023396__extent_competed RENAME TO idx_a2023396__extent_competed_old;
ALTER INDEX IF EXISTS idx_a2023396__type_set_aside RENAME TO idx_a2023396__type_set_aside_old;
ALTER INDEX IF EXISTS idx_a2023396__product_or_service_code RENAME TO idx_a2023396__product_or_service_code_old;
ALTER INDEX IF EXISTS idx_a2023396__gin_naics_code RENAME TO idx_a2023396__gin_naics_code_old;
ALTER INDEX IF EXISTS idx_a2023396__naics_code RENAME TO idx_a2023396__naics_code_old;
ALTER INDEX IF EXISTS idx_a2023396__business_categories RENAME TO idx_a2023396__business_categories_old;
ALTER INDEX IF EXISTS idx_a2023396__keyword_string RENAME TO idx_a2023396__keyword_string_old;
ALTER INDEX IF EXISTS idx_a2023396__award_id_string RENAME TO idx_a2023396__award_id_string_old;
ALTER INDEX IF EXISTS idx_a2023396__tuned_type_and_idv RENAME TO idx_a2023396__tuned_type_and_idv_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_pop_1 RENAME TO idx_a2023396__compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_pop_2 RENAME TO idx_a2023396__compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_pop_3 RENAME TO idx_a2023396__compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_rl_1 RENAME TO idx_a2023396__compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_rl_2 RENAME TO idx_a2023396__compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_a2023396__compound_geo_rl_3 RENAME TO idx_a2023396__compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW universal_transaction_matview_temp RENAME TO universal_transaction_matview;
ALTER INDEX idx_a2023396__transaction_id_temp RENAME TO idx_a2023396__transaction_id;
ALTER INDEX idx_a2023396__action_date_temp RENAME TO idx_a2023396__action_date;
ALTER INDEX idx_a2023396__fiscal_year_temp RENAME TO idx_a2023396__fiscal_year;
ALTER INDEX idx_a2023396__type_temp RENAME TO idx_a2023396__type;
ALTER INDEX idx_a2023396__ordered_type_temp RENAME TO idx_a2023396__ordered_type;
ALTER INDEX idx_a2023396__action_type_temp RENAME TO idx_a2023396__action_type;
ALTER INDEX idx_a2023396__award_id_temp RENAME TO idx_a2023396__award_id;
ALTER INDEX idx_a2023396__award_category_temp RENAME TO idx_a2023396__award_category;
ALTER INDEX idx_a2023396__total_obligation_temp RENAME TO idx_a2023396__total_obligation;
ALTER INDEX idx_a2023396__ordered_total_obligation_temp RENAME TO idx_a2023396__ordered_total_obligation;
ALTER INDEX idx_a2023396__total_obl_bin_temp RENAME TO idx_a2023396__total_obl_bin;
ALTER INDEX idx_a2023396__total_subsidy_cost_temp RENAME TO idx_a2023396__total_subsidy_cost;
ALTER INDEX idx_a2023396__ordered_total_subsidy_cost_temp RENAME TO idx_a2023396__ordered_total_subsidy_cost;
ALTER INDEX idx_a2023396__pop_country_code_temp RENAME TO idx_a2023396__pop_country_code;
ALTER INDEX idx_a2023396__pop_state_code_temp RENAME TO idx_a2023396__pop_state_code;
ALTER INDEX idx_a2023396__pop_county_code_temp RENAME TO idx_a2023396__pop_county_code;
ALTER INDEX idx_a2023396__pop_zip5_temp RENAME TO idx_a2023396__pop_zip5;
ALTER INDEX idx_a2023396__pop_congressional_code_temp RENAME TO idx_a2023396__pop_congressional_code;
ALTER INDEX idx_a2023396__gin_recipient_name_temp RENAME TO idx_a2023396__gin_recipient_name;
ALTER INDEX idx_a2023396__gin_recipient_unique_id_temp RENAME TO idx_a2023396__gin_recipient_unique_id;
ALTER INDEX idx_a2023396__gin_parent_recipient_unique_id_temp RENAME TO idx_a2023396__gin_parent_recipient_unique_id;
ALTER INDEX idx_a2023396__recipient_id_temp RENAME TO idx_a2023396__recipient_id;
ALTER INDEX idx_a2023396__recipient_name_temp RENAME TO idx_a2023396__recipient_name;
ALTER INDEX idx_a2023396__recipient_unique_id_temp RENAME TO idx_a2023396__recipient_unique_id;
ALTER INDEX idx_a2023396__parent_recipient_unique_id_temp RENAME TO idx_a2023396__parent_recipient_unique_id;
ALTER INDEX idx_a2023396__awarding_agency_id_temp RENAME TO idx_a2023396__awarding_agency_id;
ALTER INDEX idx_a2023396__funding_agency_id_temp RENAME TO idx_a2023396__funding_agency_id;
ALTER INDEX idx_a2023396__awarding_toptier_agency_name_temp RENAME TO idx_a2023396__awarding_toptier_agency_name;
ALTER INDEX idx_a2023396__awarding_subtier_agency_name_temp RENAME TO idx_a2023396__awarding_subtier_agency_name;
ALTER INDEX idx_a2023396__funding_toptier_agency_name_temp RENAME TO idx_a2023396__funding_toptier_agency_name;
ALTER INDEX idx_a2023396__funding_subtier_agency_name_temp RENAME TO idx_a2023396__funding_subtier_agency_name;
ALTER INDEX idx_a2023396__recipient_location_country_code_temp RENAME TO idx_a2023396__recipient_location_country_code;
ALTER INDEX idx_a2023396__recipient_location_state_code_temp RENAME TO idx_a2023396__recipient_location_state_code;
ALTER INDEX idx_a2023396__recipient_location_county_code_temp RENAME TO idx_a2023396__recipient_location_county_code;
ALTER INDEX idx_a2023396__recipient_location_zip5_temp RENAME TO idx_a2023396__recipient_location_zip5;
ALTER INDEX idx_a2023396__recipient_location_congressional_code_temp RENAME TO idx_a2023396__recipient_location_congressional_code;
ALTER INDEX idx_a2023396__cfda_multi_temp RENAME TO idx_a2023396__cfda_multi;
ALTER INDEX idx_a2023396__pulled_from_temp RENAME TO idx_a2023396__pulled_from;
ALTER INDEX idx_a2023396__type_of_contract_pricing_temp RENAME TO idx_a2023396__type_of_contract_pricing;
ALTER INDEX idx_a2023396__extent_competed_temp RENAME TO idx_a2023396__extent_competed;
ALTER INDEX idx_a2023396__type_set_aside_temp RENAME TO idx_a2023396__type_set_aside;
ALTER INDEX idx_a2023396__product_or_service_code_temp RENAME TO idx_a2023396__product_or_service_code;
ALTER INDEX idx_a2023396__gin_naics_code_temp RENAME TO idx_a2023396__gin_naics_code;
ALTER INDEX idx_a2023396__naics_code_temp RENAME TO idx_a2023396__naics_code;
ALTER INDEX idx_a2023396__business_categories_temp RENAME TO idx_a2023396__business_categories;
ALTER INDEX idx_a2023396__keyword_string_temp RENAME TO idx_a2023396__keyword_string;
ALTER INDEX idx_a2023396__award_id_string_temp RENAME TO idx_a2023396__award_id_string;
ALTER INDEX idx_a2023396__tuned_type_and_idv_temp RENAME TO idx_a2023396__tuned_type_and_idv;
ALTER INDEX idx_a2023396__compound_geo_pop_1_temp RENAME TO idx_a2023396__compound_geo_pop_1;
ALTER INDEX idx_a2023396__compound_geo_pop_2_temp RENAME TO idx_a2023396__compound_geo_pop_2;
ALTER INDEX idx_a2023396__compound_geo_pop_3_temp RENAME TO idx_a2023396__compound_geo_pop_3;
ALTER INDEX idx_a2023396__compound_geo_rl_1_temp RENAME TO idx_a2023396__compound_geo_rl_1;
ALTER INDEX idx_a2023396__compound_geo_rl_2_temp RENAME TO idx_a2023396__compound_geo_rl_2;
ALTER INDEX idx_a2023396__compound_geo_rl_3_temp RENAME TO idx_a2023396__compound_geo_rl_3;
