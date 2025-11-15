from django.urls import path
from . import views

app_name = "summariser"

urlpatterns = [
    # Main list / UI page: combined filters (search, date, source)
    path("", views.summaries, name="summaries"),

    # Optional: endpoint to manually trigger a scrape or refresh
    # (only include this if you already have a fetch or import view)
    # path("fetch/", views.fetch_latest, name="fetch-latest"),

    # Optional: API endpoint for JSON summaries (for JS front-end use)
    # path("api/", views.summaries_api, name="summaries-api"),
]
