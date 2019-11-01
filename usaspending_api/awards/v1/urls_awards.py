from django.conf.urls import url

from usaspending_api.awards.v1 import views

# map reqest types to viewset method; replace this with a router
award_list = views.AwardListViewSet.as_view({"get": "list", "post": "list"})
award_total = views.AwardAggregateViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [
    url(r"^$", award_list, name="award-list"),
    url(r"^total/", award_total, name="award-total"),
]
