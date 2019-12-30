"""
Look ups for elasticsearch fields to be displayed for the front end
"""
from copy import deepcopy
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings


TRANSACTIONS_LOOKUP = {
    "Recipient Name": "recipient_name.keyword",
    "Action Date": "action_date",
    "Transaction Amount": "transaction_amount",
    "Award Type": "type_description.keyword",
    "Awarding Agency": "awarding_toptier_agency_name.keyword",
    "Awarding Sub Agency": "awarding_subtier_agency_name.keyword",
    "Funding Agency": "funding_toptier_agency_name",
    "Funding Sub Agency": "funding_subtier_agency_name",
    "Issued Date": "period_of_performance_start_date",
    "Loan Value": "face_value_loan_guarantee",
    "Subsidy Cost": "original_loan_subsidy_cost",
    "Mod": "modification_number",
    "Award ID": "display_award_id",
    "awarding_agency_id": "awarding_agency_id",
    "internal_id": "award_id",
    "generated_internal_id": "generated_unique_award_id",
    "Last Date to Order": "ordering_period_end_date",
}

TRANSACTIONS_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in TRANSACTIONS_LOOKUP.items()}

INDEX_ALIASES_TO_AWARD_TYPES = deepcopy(all_award_types_mappings)
INDEX_ALIASES_TO_AWARD_TYPES["directpayments"] = INDEX_ALIASES_TO_AWARD_TYPES.pop("direct_payments")
INDEX_ALIASES_TO_AWARD_TYPES["other"] = INDEX_ALIASES_TO_AWARD_TYPES.pop("other_financial_assistance")

KEYWORD_DATATYPE_FIELDS = [
    "recipient_name.keyword",
    "awarding_toptier_agency_name.keyword",
    "awarding_subtier_agency_name.keyword",
    "type_description.keyword",
]
