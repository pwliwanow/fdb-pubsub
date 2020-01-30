import Dependencies._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin.autoImport._

addCommandAlias("testAll", ";test;it:test")

lazy val scala2_12 = "2.12.10"
lazy val scala2_13 = "2.13.1"
lazy val supportedScalaVersions = List(scala2_12, scala2_13)

ThisBuild / scalaVersion := scala2_12

lazy val fdbPubSub = project
  .in(file("."))
  .settings(
    commonSettings,
    skip in publish := true,
    crossScalaVersions := Nil
  )
  .aggregate(pubSub, example)

lazy val pubSub = project
  .in(file("pubsub"))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    name := "pubsub",
    libraryDependencies ++= allDeps,
    crossScalaVersions := supportedScalaVersions
  )

lazy val example = project
  .in(file("example"))
  .dependsOn(pubSub)
  .settings(
    commonSettings,
    name := "example",
    skip in publish := true,
    coverageEnabled := false,
    crossScalaVersions := supportedScalaVersions
  )

lazy val commonSettings =
  ossPublishSettings ++
    Defaults.itSettings ++
    Seq(
      organization := "com.github.pwliwanow.fdb-pubsub",
      scalafmtOnCompile := true,
      autoCompilerPlugins := true,
      addCompilerPlugin("com.lihaoyi" %% "acyclic" % acyclicVersion),
      parallelExecution in ThisBuild := false,
      fork := true,
      scalacOptions ++= {
        val commonScalacOptions =
          List(
            "-unchecked",
            "-deprecation",
            "-encoding",
            "UTF-8",
            "-explaintypes",
            "-feature",
            "-language:higherKinds",
            "-language:implicitConversions",
            "-Xfatal-warnings",
            "-Xlint:inaccessible",
            "-Xlint:infer-any",
            "-Ywarn-dead-code",
            "-Ywarn-unused:implicits",
            "-Ywarn-unused:imports",
            "-Ywarn-unused:locals",
            "-Ywarn-unused:params",
            "-Ywarn-unused:patvars",
            "-Ywarn-unused:privates",
            "-P:acyclic:force"
          )
        val extraOptions =
          if (scalaVersion.value == scala2_12) List("-Ypartial-unification")
          else List.empty
        commonScalacOptions ++ extraOptions
      },
      scalacOptions in (Compile, doc) ++= Seq(
        "-no-link-warnings"
      )
    )

lazy val ossPublishSettings = Seq(
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),
  publishArtifact in Test := false,
  publishMavenStyle := true,
  sonatypeProfileName := "com.github.pwliwanow",
  pomIncludeRepository := { _ =>
    false
  },
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  organizationHomepage := Some(url("https://github.com/pwliwanow/fdb-pubsub")),
  homepage := Some(url("https://github.com/pwliwanow/fdb-pubsub")),
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/pwliwanow/fdb-pubsub"),
      "scm:git:https://github.com/pwliwanow/fdb-pubsub.git"
    )
  ),
  autoAPIMappings := true,
  developers := List(
    Developer(
      id = "pwliwanow",
      name = "Pawel Iwanow",
      email = "pwliwanow@gmail.com",
      url = new URL("https://github.com/pwliwanow/")
    )
  ),
  // sbt-release
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseIgnoreUntrackedFiles := true,
  releaseProcess := Seq(
    checkSnapshotDependencies,
    inquireVersions,
    // publishing locally so that the pgp password prompt is displayed early
    // in the process
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    releaseStepCommandAndRemaining("+clean"),
    releaseStepCommandAndRemaining("+test"),
    releaseStepCommandAndRemaining("+it:test"),
    setReleaseVersion,
    releaseStepTask(updateVersionInReadme),
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"),
    setNextVersion,
    commitNextVersion,
    releaseStepCommand("sonatypeRelease"),
    pushChanges
  )
)

lazy val updateVersionInReadme =
  taskKey[Unit]("Updates version in README.md to the one present in version.sbt")

updateVersionInReadme := {
  Versions.replaceVersionInReadmeForSbt(version.value)
  Versions.replaceVersionInReadmeForMaven(version.value)
}
