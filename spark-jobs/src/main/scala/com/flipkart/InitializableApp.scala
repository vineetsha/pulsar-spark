package com.flipkart

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Created by sharma.varun
 */
trait InitializableApp {

  protected val isInitialized = new AtomicBoolean(false)

  def getInitStatus:Boolean = isInitialized.get()

  def init()
  def shutdown()

  def noop() = {}




}


