package com.cultureamp.common

sealed class Action {
    data object Wait : Action()
    data object Continue : Action()
    data object Stop : Action()
    data class Error(val exception: Throwable) : Action()
}

