/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
package org.avant.spark3

import org.avant.spark3.Spark3Launch
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Spark3LaunchSuite extends AnyFunSuite {
  test("App has a greeting") {
    assert(Spark3Launch.greeting() != null)
  }
}