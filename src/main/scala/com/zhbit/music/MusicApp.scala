package com.zhbit.music

import com.zhbit.music.dealData.{CalData, GetDataFdfs}

object MusicApp {

  val action = new CalData

  def main(args: Array[String]): Unit = {

    action.evaluateModel2()

  }
}
