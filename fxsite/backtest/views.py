from django.shortcuts import render


def index(request):
    # 変数設定
    params = {"message_me": "Hello World"}
    # 出力
    return render(request, "main/index.html", context=params)
