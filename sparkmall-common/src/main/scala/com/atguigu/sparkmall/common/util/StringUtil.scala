package com.atguigu.sparkmall.common.util

object StringUtil {

    def isNotEmpty(s: String): Boolean = {
        s != null && !"".equals(s.trim)
    }

}
