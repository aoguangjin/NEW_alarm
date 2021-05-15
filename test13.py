# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/25 17:17
# ☯ File  : test13.py
# 曾称质数。一个大于1的正整数，如果除了1和它本身以外，不能被其他正整数整除，就叫素数。如2，3，5，7，11，13，17…。
i = 2
while (i < 10):
    j = 2

    while j <= (i / j):
        print(i % j)
        if not (i % j):
            break
        j = j + 1

    if (j > i / j):
        print(i, " 是素数")

    i = i + 1