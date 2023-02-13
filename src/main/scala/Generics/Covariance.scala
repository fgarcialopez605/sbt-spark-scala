package Generics

object Covariance extends App {

  abstract class Flower {
    def name: String
  }

  // Creating a sub-class Lily
  // of Flower
  case class Lily(name: String) extends Flower

  // Creating a sub-class Carnation
  // of Flower
  case class Carnation(name: String) extends Flower

  // Creating a method
  def FlowerNames(flowers: List[Flower]): Unit = {
    flowers.foreach {
      flower => println(flower.name)
    }
  }

  // Assigning names
  val lily: List[Lily] = List(Lily("White Lily"),
    Lily("Jersey Lily"))
  val carnations: List[Carnation] = List(Carnation("White carnations"),
    Carnation("Pink carnations"))

  // Print: names of lily
  FlowerNames(lily)

  // Print: names of carnation
  FlowerNames(carnations)
}
