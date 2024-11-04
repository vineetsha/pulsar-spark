package com.flipkart.core

import com.flipkart.config.Config
import com.flipkart.kloud.authn.AuthTokenService
import com.flipkart.security.cryptex.CryptexClientBuilder
import org.slf4j.LoggerFactory


object CryptexInitializer {
  private val LOGGER = LoggerFactory.getLogger(classOf[CryptexInitializer])
}

class CryptexInitializer() {
  AuthTokenService.init(getAuthnUrl, getClientId, getClientSecret)
  var cryptexClient = new CryptexClientBuilder(AuthTokenService.getInstance).build

  def decrypt(key: String): String = {
    CryptexInitializer.LOGGER.info("Decrypting key {}", key)
    cryptexClient.decrypt(key)
  }

  private[core] def getAuthnUrl = Config.getProperty("cryptex.authnUrl", "http://10.24.0.165")

  private[core] def getClientId = Config.getProperty("cryptex.clientId", "flip-dev")

  private[core] def getClientSecret = Config.getProperty("cryptex.clientSecret", "Nrdq5PRkHgWh4bJUQzb7KOi+Dmmupl4qLeTgplzW1MNrtT2C")

  def isCryptexEnabled: Boolean = !"false".equalsIgnoreCase(Config.getProperty("cryptex.enabled", "false"))
}
