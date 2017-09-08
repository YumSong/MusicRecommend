package com.zhbit.music

import com.zhbit.music.kafka.Data2Streaming

object StreamingGet {

  def main(args: Array[String]): Unit = {

    val d2s = new Data2Streaming

    d2s.getData()

  }

}
