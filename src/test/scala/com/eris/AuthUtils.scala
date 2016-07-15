package com.eris

import java.io.{File, FileInputStream}
import java.security.KeyStore

import com.eris.PathUtils._
import com.jayway.restassured.RestAssured
import com.jayway.restassured.config.SSLConfig
import com.jayway.restassured.http.ContentType
import com.jayway.restassured.response.Response
import org.apache.http.conn.ssl.SSLSocketFactory
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, Suite}
import org.scalatest.concurrent.Eventually
import org.scalatest.selenium.WebBrowser

import scala.util.control.Breaks._

trait AuthUtils extends Config with Matchers with BeforeAndAfter with BeforeAndAfterAll with Eventually with WebBrowser { this: Suite =>

  var tokenMerchantW: String
  var tokenMerchantV: String
  var tokenGoodclub: String

  override def beforeAll(): Unit = {
    tokenMerchantW = getAzureToken("bobby@BobbyTestApp.onmicrosoft.com","3RisT35t!12", azureAuthExecutor())
    tokenMerchantV = getAzureToken("jarnotest@BobbyTestApp.onmicrosoft.com", "3RisT35t!22", azureAuthExecutor())
    tokenGoodclub = getAzureToken("goodclub-eris@goodclub.nl", "goodclub123", azureAuthExecutor())
  }

  def getWithAuth(uri: String, validAuthenticationToken: String): Response =

    RestAssured
      .given
      .header("Authorization", validAuthenticationToken)
      .expect
      .response
      .when
      .request
      .get(uri)

  def getWithAuthMtoM(uri: String): Response =

    RestAssured
      .given
      .expect
      .response
      .when
      .request
      .get(uri)

  def postWithAuth(uri: String, dagDefinition: String, validAuthenticationToken: String): Response = {
    RestAssured
      .given
      .header("Authorization", validAuthenticationToken)
      .contentType(ContentType.JSON)
      .body(dagDefinition)
      .when
      .request
      .post(uri)
  }

  def getAzureToken(username: String, password: String, endpoint: String): String = {

    //implicit val webDriver: WebDriver = new ChromeDriver()
    implicit val webDriver = new PhantomJSDriver()

    val host = endpoint
    go to (host)
    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      emailField("login").isDisplayed shouldBe true
      emailField("login").value = username
      emailField("login").value should be (username)
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      pwdField("passwd").isDisplayed shouldBe true
      pwdField("passwd").value = password
      pwdField("passwd").value should be (password)
    }

    implicitlyWait(Span(1000, Millis))

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      find("cred_sign_in_button")
      val url = currentUrl
      println("url: " + url)
      breakable { for(i <- 1 to 5){
        click on "cred_sign_in_button"
        println("current: " + currentUrl)
        implicitlyWait(Span(800, Millis))
        if(currentUrl != url) break
      }}
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      currentUrl should include("key=")
    }
    val cacheKeyUrl = currentUrl
    val cacheKeySplit = cacheKeyUrl.split("""key=""")
    val cacheKey = cacheKeySplit(1)

    val cacheKeyHost =
      if(endpoint == azureAuthExecutor()) {
        azureExecutorCacheKey(cacheKey)
      }else{
        azureStorageCacheKey(cacheKey)
      }

    go to (cacheKeyHost)

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      pageSource should include("""token":"""")
    }

    val fullSource = pageSource
    fullSource should include("""token":"""")

    val splitAtToken = fullSource.split("""token":"""")
    val splitAfterToken = splitAtToken(1).split( """"}""")
    val fullToken = splitAfterToken(0)
    quit()

    fullToken

  }

  def getAzureTokenWithInvalidEmail(username: String, password: String, endpoint: String): String = {

    //implicit val webDriver: WebDriver = new ChromeDriver()
    implicit val webDriver = new PhantomJSDriver()

    val host = endpoint
    print(host)
    go to (host)

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      emailField("login").isDisplayed shouldBe true
      emailField("login").value = username
      emailField("login").value should be(username)
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      pwdField("passwd").isDisplayed shouldBe true
      click on pwdField("passwd")
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))){
      pageTitle should include("Sign in to your Microsoft account")
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      pwdField("passwd").isDisplayed shouldBe true
      pwdField("passwd").value = password
      pwdField("passwd").value should be(password)
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      click on "idSIButton9"
    }

    eventually(timeout(Span(10000, Millis)), interval(Span(200, Millis))) {
      pageSource shouldNot include("token")
    }

    val fullSource = pageSource
    val splitAtText = fullSource.split( """pre-wrap;">""")
    val splitAfterText = splitAtText(1).split( """</pre>""")
    val fullText = splitAfterText(0)
    quit()

    fullText


  }

  def setUpSSL(KeyStoreLoc: String, KeyPass: String, CertLoc: String, CertPass: String): Boolean = {

    // load the corresponding keystores
    val privateKeyStoreLocation = new FileInputStream (new File ("src/test/resources/keystores/" + KeyStoreLoc) )
    val keyStore = KeyStore.getInstance ("PKCS12")
    keyStore.load (privateKeyStoreLocation, KeyPass.toCharArray () )

    val certKeyStoreLocation = new FileInputStream (new File ("src/test/resources/keystores/" + CertLoc) )
    val trustStore = KeyStore.getInstance ("jks")
    trustStore.load (certKeyStoreLocation, CertPass.toCharArray () )

    // manually create a new sockerfactory and pass in the required values
    val clientAuthFactory = new org.apache.http.conn.ssl.SSLSocketFactory (keyStore, KeyPass, trustStore)
    // don't check on hostname
    clientAuthFactory.setHostnameVerifier (SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)

    // set the config in rest assured
    val config = new SSLConfig ().`with` ().sslSocketFactory (clientAuthFactory).and ().allowAllHostnames ()
    RestAssured.config = RestAssured.config ().sslConfig (config)

    true
  }
}
