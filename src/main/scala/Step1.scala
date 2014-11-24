import akka.actor.ActorSystem
import akka.stream.{FlowMaterializer, MaterializerSettings}
import akka.stream.scaladsl.Flow
import org.reactivestreams.api.Producer
import scala.concurrent.duration._

case object Tick

object Merge {

  def apply[T](streams: Seq[Producer[_ <: T]], mat: FlowMaterializer): Producer[T] = {
    def rec(level: Int, p: Producer[T], s: Seq[Producer[T]]): Producer[T] =
      if (s.isEmpty) p
      else {
        val toMerge = if (level == 1) s.head else rec(1, s.head, s.tail.take(level - 1))
        rec(level * 2, Flow(p).merge(toMerge).toProducer(mat), s.drop(level))
      }
    val cast = streams.asInstanceOf[Seq[Producer[T]]] // due to Java invariance
    rec(1, cast.head, cast.tail)
  }

}

/**
 * Created by markmo on 23/08/2014.
 */
object Step1 extends App {
  import Bank._

  case class Summary(num: Int, amount: Long) {
    def +(t: Transfer) = Summary(num + 1, amount + t.amount)
  }

  object Summary {
    def apply(t: Transfer): Summary = Summary(1, t.amount)
  }

  implicit val sys = ActorSystem("Step1")
  implicit val ec = sys.dispatcher
  implicit val sched = sys.scheduler
  val mat = FlowMaterializer(MaterializerSettings(initialInputBufferSize = 2))

//  Flow(() => transfer()).take(10).foreach(println).onComplete(mat)(_ => sys.shutdown())
//  println("done")
  val input = Flow(() => transfer()).toProducer(mat)

  // rate limit to 1/sec
  val ticks = Flow(1.second, () => Tick)

  val rateFlow = Flow(5.seconds, {
    var current = 1.0

    () => {
      current *= 1.5
      rates(current)
    }
  })


//  ticks.zip(input).foreach(println).consume(mat)

  // ask WebService for currency exchange rate
//  ticks.zip(input).mapFuture {
//    case (_, t) => WebService.convertToEUR(t.currency, t.amount)
//      .map(amount => t -> Transfer(t.from, t.to, Currency("EUR"), amount))
//  }.foreach(println).consume(mat)

//  val summarized = Flow(input).mapFuture {
//    t => WebService.convertToEUR(t.currency, t.amount)
//      .map(Transfer(t.from, t.to, Currency("EUR"), _))
//  }.conflate[Summary](Summary(_), _ + _).toProducer(mat)
//  ticks.zip(summarized).foreach(println).consume(mat)

//  val summarized: Producer[Summary] = rateFlow
//    .expand(identity, (r: Map[Currency, Double]) => r -> r)
//    .zip(input)
//    .map { case (rates, t) => Transfer(t.from, t.to, Currency("EUR"), (t.amount / rates(t.currency)).toLong) }
//    .conflate[Summary](Summary(_), _ + _)
//    .toProducer(mat)
//
//  ticks.zip(summarized).foreach {
//    case (_, Summary(n, amount)) => println(s"$n transfers, amounting to $amount")
//  }.consume(mat)

  val streams =
    for (_ <- 1 to 50000) yield ticks.zip(input).map(x => x._2).mapFuture { t =>
      WebService.convertToEUR(t.currency, t.amount)
        .map(amount => Transfer(t.from, t.to, Currency("EUR"), amount))
    }.toProducer(mat)

  Flow(Merge(streams, mat))
    .groupedWithin(1000000, 1.second)
    .map(analyze).foreach(println)
    .consume(mat)

  def rates(factor: Double): Map[Currency, Double] = WebService.rates.mapValues(_ * factor)

  private def analyze(transfers: Seq[Transfer]): String = {
    val num = transfers.size
    val avg = transfers.map(_.amount).sum / num
    s"$num transfers averaging $avg EUR"
  }

}
