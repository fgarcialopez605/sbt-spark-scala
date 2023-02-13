package Generics

object ScalaContravariance extends App {

    val superType = new SuperType
    val subType = new SubType

    val typeCarer = new TypeCarer

    typeCarer.display(subType)
    typeCarer.display(superType)

  abstract class Type[-T] {
    def typeName(): Unit
  }

  class SuperType extends Type[AnyVal] {
    override def typeName(): Unit = {
      print("\n\n SuperType \n\n")
    }
  }

  class SubType extends Type[Int] {
    override def typeName(): Unit = {
      print("\n\n SubType \n\n")
    }
  }

  class TypeCarer {
    def display(t: Type[Int]) {
      t.typeName()
    }
  }

}
