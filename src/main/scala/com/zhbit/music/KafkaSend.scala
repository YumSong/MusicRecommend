package com.zhbit.music

import com.zhbit.music.kafka.ProducterData

object KafkaSend {

  def main(args: Array[String]): Unit = {

    val pd = new ProducterData

    pd.sendData()

  }

}
