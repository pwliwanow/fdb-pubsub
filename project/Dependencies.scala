import sbt._

object Dependencies {
  private val testScopes = List(IntegrationTest, Test)

  val acyclicVersion = "0.2.0"
  private lazy val acyclic = "com.lihaoyi" %% "acyclic" % acyclicVersion % "provided"

  private val akkaVersion = "2.6.4"
  private lazy val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  private lazy val akkaStreamsTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

  private val fdb4sVersion = "0.11.0"
  private lazy val fdb4sCore = "com.github.pwliwanow.foundationdb4s" %% "core" % fdb4sVersion
  private lazy val fdb4sAkkaStreams = "com.github.pwliwanow.foundationdb4s" %% "akka-streams" % fdb4sVersion

  private lazy val dependencies: Seq[ModuleID] =
    Seq(acyclic, fdb4sAkkaStreams, fdb4sCore)

  private val scalaTestVersion = "3.1.2"
  private lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  private lazy val testDeps: Seq[ModuleID] =
    for {
      dep <- Seq(akkaTestKit, akkaStreamsTestKit, scalaTest)
      testScope <- testScopes
    } yield dep % testScope

  lazy val allDeps: Seq[ModuleID] = dependencies ++ testDeps
}
