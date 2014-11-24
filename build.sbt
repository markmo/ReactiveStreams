name := "ReactiveStreams"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.5",
  "org.reactivestreams" % "reactive-streams-spi" % "0.3",
  "com.typesafe.akka" %% "akka-stream-experimental" % "0.3"
)