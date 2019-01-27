import Dependencies._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin.autoImport._

addCommandAlias("testAll", ";test;it:test")

lazy val fdbPubSub = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(skip in publish := true)
  .aggregate(pubSub, example)

lazy val pubSub = project
  .in(file("pubsub"))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    name := "pubsub",
    libraryDependencies ++= allDeps
  )

lazy val example = project
  .in(file("example"))
  .dependsOn(pubSub)
  .settings(
    commonSettings,
    name := "example",
    skip in publish := true
  )

lazy val commonSettings =
  buildSettings ++
    Defaults.itSettings ++
    Seq(
      organization := "com.github.pwliwanow.fdb-pubsub",
      scalaVersion := "2.12.8",
      scalafmtOnCompile := true,
      coverageExcludedPackages := "com.github.pwliwanow.fdb.pubsub.example.*",
      parallelExecution in ThisBuild := false,
      releaseProcess := Seq(
        checkSnapshotDependencies,
        inquireVersions,
        // publishing locally so that the pgp password prompt is displayed early
        // in the process
        releaseStepCommandAndRemaining("+publishLocalSigned"),
        releaseStepCommandAndRemaining("+clean"),
        releaseStepCommandAndRemaining("+test"),
        setReleaseVersion,
        releaseStepTask(updateVersionInReadme),
        commitReleaseVersion,
        tagRelease,
        releaseStepCommandAndRemaining("+publishSigned"),
        setNextVersion,
        commitNextVersion,
        releaseStepCommand("sonatypeRelease"),
        pushChanges
      ),
      parallelExecution in ThisBuild := false,
      fork := true,
      scalacOptions ++= Seq(
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
        "-Ypartial-unification",
        "-Ywarn-unused:implicits",
        "-Ywarn-unused:imports",
        "-Ywarn-unused:locals",
        "-Ywarn-unused:params",
        "-Ywarn-unused:patvars",
        "-Ywarn-unused:privates"
      ),
      scalacOptions in (Compile, doc) ++= Seq(
        "-no-link-warnings"
      )
    )

lazy val buildSettings =
  commonSmlBuildSettings ++
    acyclicSettings ++
    ossPublishSettings

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
  releaseProcess := Release.steps(organization.value)
)

lazy val updateVersionInReadme =
  taskKey[Unit]("Updates version in README.md to the one present in version.sbt")

updateVersionInReadme := {
  Versions.replaceVersionInReadmeForSbt(version.value)
  Versions.replaceVersionInReadmeForMaven(version.value)
}
