import java.util.concurrent.atomic.AtomicReference

import com.itv.bucky
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl.{Declaration, DeclarationExecutor, Exchange, Queue}
import com.itv.bucky.lifecycle.AmqpClientLifecycle
import com.itv.bucky.pattern.requeue.{RequeuePolicy, _}
import com.itv.bucky.{Ack, AmqpClientConfig, ExchangeName, QueueName, RequeueHandler, RoutingKey, _}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.concurrent.Eventually
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class MessagePriorityTest extends FunSuite with Matchers with TypeCheckedTripleEquals {

  test("Messages should be received in order of priority") {

    val amqpClientConfig = AmqpClientConfig("localhost", 5672, "guest", "guest")

    val queueName = QueueName("test.queue")
    val queue = Queue(queueName, arguments = Map("x-max-priority" -> Int.box(10)))
    val routingKey = RoutingKey("rk")
    val exchange = ExchangeName("exchange")
    val declarations: List[Declaration] = List(queue, Exchange(exchange).binding(routingKey -> queueName))

    val messagesReceived = new AtomicReference[List[String]](List.empty)

    val handler = new RequeueHandler[Future, String] {
      override def apply(msg: String): Future[bucky.RequeueConsumeAction] = {
        Future(println(s"Received message: $msg")).map(_ => messagesReceived.updateAndGet(_ :+ msg)).map(_ => Ack)
      }
    }

    def priorityPublisherBuilder(priority: Int): PublishCommandBuilder[String] = (msg: String) => {
      val messageProperties = MessageProperties.persistentBasic.copy(priority = Some(priority))
      PublishCommand(exchange, routingKey, messageProperties, Payload.from(msg))
    }

    val test = for {
      client <- AmqpClientLifecycle.apply(amqpClientConfig)
      _ = DeclarationExecutor.apply(declarations, client)
      highPriorityPublisher <- client.publisherOf(priorityPublisherBuilder(10))
      mediumPriorityPublisher <- client.publisherOf(priorityPublisherBuilder(5))
      lowPriorityPublisher <- client.publisherOf(priorityPublisherBuilder(0))
      _ = lowPriorityPublisher("Low Priority")
      _ = highPriorityPublisher("High Priority")
      _ = mediumPriorityPublisher("Middle Priority")
      _ <- RequeueOps(client).requeueHandlerOf[String](queueName, handler, RequeuePolicy(maximumProcessAttempts = 10, 3.minute), StringPayloadUnmarshaller, prefetchCount = 1)
    } yield {
      Eventually.eventually {
        messagesReceived.get() should ===(List("High Priority", "Middle Priority", "Low Priority"))
      }
    }
    test.runUntilJvmShutdown()
  }
}
