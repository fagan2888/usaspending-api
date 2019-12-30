from django.contrib.postgres.aggregates import StringAgg
from usaspending_api.common.helpers.orm_helpers import FiscalYear
from usaspending_api.settings import HOST
from django.db.models.functions import Concat
from django.db.models import Func, F, Value, Subquery, TextField, OuterRef
from usaspending_api.awards.models import Award

AWARD_URL = f"{HOST}/#/award/" if "localhost" in HOST else f"https://{HOST}/#/award/"


def universal_transaction_matview_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "transaction__award__financial_set__treasury_account__federal_account__federal_account_code",
            ";",
            distinct=True,
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("transaction__award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
    }
    return annotation_fields


def universal_award_matview_annotations():
    annotation_fields = {
        "treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
    }
    return annotation_fields


def idv_order_annotations():
    annotation_fields = {
        "treasury_accounts_funding_this_award": StringAgg(
            "financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
    }
    return annotation_fields


def idv_transaction_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
    }
    return annotation_fields


def subaward_annotations():
    annotation_fields = {
        "subaward_action_date_fiscal_year": FiscalYear("subaward__action_date"),
        "prime_award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "prime_award_federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "prime_award_treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
    }
    return annotation_fields
