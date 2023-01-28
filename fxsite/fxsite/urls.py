from django.urls import include, path

urlpatterns = [
    path("backtest/", include("backtest.urls")),
]
