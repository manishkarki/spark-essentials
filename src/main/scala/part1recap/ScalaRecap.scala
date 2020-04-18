package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * @author mkarki
  */
object ScalaRecap extends App {

  val aBoolean: Boolean = false

  //expressions
  val anIfExpn = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, Scala") //Unit = no meaningful value or void

  //functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  //trait are interface
  trait Carnivore {
    def eat(animal: Animal) : Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crocodile")
  }

  //singleton object
  object MySingleton
  //companions are object and trait with same name
  object Carnivore
  //generics
  trait MyList[+A]
  //pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _=> "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e : NullPointerException => "some returned value"
  }
  //comprehensions( syntatic sugar for map, filter etc)

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    //some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I found $meaningOfLife")
    case Failure(exception) => println(s"I have failed: $exception")
  }

  //partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  //implicits
  // auto injection by the container
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  //compiler makes the implicit call
  val implicitCall = methodWithImplicitArgument

  //implicit conversions -implicit defs
  case class Person(name: String) {
    def greet =  println(s"Hi, my name is $name")
  }

  case class TestIntegerAddition(x: Int) {
    def add = x + 1
  }

  implicit def addNumber(x: Int): TestIntegerAddition = TestIntegerAddition(x)

  implicit def fromStringToPerson(name: String): Person = Person(name)

  "Manish".greet // fromStringToPerson is called implicitly by compiler
  4.add

  //implicit conversion is done by implicit classes

  /*
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
   */
}
