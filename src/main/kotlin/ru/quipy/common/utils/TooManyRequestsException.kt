package ru.quipy.common.utils

class TooManyRequestsException(val delay: Int = 1) : Exception("Too many requests")