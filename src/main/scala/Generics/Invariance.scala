package Generics

object Invariance extends App {

  abstract class Animal {
    def name: String
  }

  case class Cat(name: String) extends Animal

  case class Dog(name: String) extends Animal

  class Container[A](value: A) {
    private var _value: A = value

    def getValue: A = _value

    def setValue(value: A): Unit = {
      _value = value
    }
  }

  val catContainer: Container[Cat] = new Container[Cat](Cat("tom"))

  // This won't compile
  // val animalContainer: Container[Animal] = catContainer
}
