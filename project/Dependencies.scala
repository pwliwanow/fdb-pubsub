import sbt._

object Dependencies {
  private val testScopes = List(IntegrationTest, Test)

  private val akkaVersion = "2.5.19"
  private lazy val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  private lazy val akkaStreamsTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

  private val fdb4sVersion = "0.6.0"
  private lazy val fdb4sCore = "com.github.pwliwanow.foundationdb4s" %% "foundationdb4s-core" % fdb4sVersion
  private lazy val fdb4sAkkaStreams = "com.github.pwliwanow.foundationdb4s" %% "foundationdb4s-akka-streams" % fdb4sVersion

  private lazy val dependencies: Seq[ModuleID] =
    Seq(fdb4sAkkaStreams, fdb4sCore)

  private val scalaTestVersion = "3.0.5"
  private lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  private val scalaMockVersion = "4.1.0"
  private lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion

  private lazy val testDeps: Seq[ModuleID] =
    for {
      dep <- Seq(akkaTestKit, akkaStreamsTestKit, scalaTest, scalaMock)
      testScope <- testScopes
    } yield dep % testScope

  lazy val allDeps: Seq[ModuleID] = dependencies ++ testDeps
}
